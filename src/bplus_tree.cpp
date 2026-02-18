/// @file bplus_tree.cpp
/// @brief B+ tree implementation with buffer pool integration and delete
///        rebalancing (redistribute / merge).

#include "bptree/bplus_tree.h"
#include "bptree/page.h"

#include <algorithm>
#include <cstring>
#include <vector>

namespace bptree {

// ============================================================================
// Construction / destruction
// ============================================================================

BPlusTree::BPlusTree(const std::string& index_file, size_t pool_size,
                     bool enable_wal)
    : disk_(std::make_unique<DiskManager>(index_file)),
      pool_(std::make_unique<BufferPool>(*disk_, pool_size))
{
    // Set up WAL if enabled.
    if (enable_wal) {
        std::string wal_path = index_file + ".wal";
        wal_ = std::make_unique<WriteAheadLog>(wal_path);

        // Run crash recovery: replay any pending page writes.
        wal_->Recover(*disk_);

        // Attach WAL to the buffer pool so flushes are logged.
        pool_->SetWAL(wal_.get());
    }

    ReadMetadata();
}

BPlusTree::~BPlusTree() {
    WriteMetadata();
    pool_->FlushAllPages();

    // Checkpoint on clean shutdown to truncate the WAL.
    if (wal_) {
        wal_->EndCheckpoint();
    }
}

// ============================================================================
// Metadata persistence (page 0 -- accessed via DiskManager directly)
// ============================================================================

void BPlusTree::WriteMetadata() {
    disk_->SetRootOffset(root_offset_);
    disk_->SetNextPageOffset(next_page_offset_);
    disk_->FlushMetadata();
}

void BPlusTree::ReadMetadata() {
    if (disk_->FileSize() >= PAGE_SIZE) {
        root_offset_      = disk_->RootOffset();
        next_page_offset_  = disk_->NextPageOffset();

        if (next_page_offset_ < static_cast<int64_t>(PAGE_SIZE)) {
            next_page_offset_ = PAGE_SIZE;
        }
        if (root_offset_ != INVALID_PAGE_ID &&
            (root_offset_ < static_cast<int64_t>(PAGE_SIZE) ||
             root_offset_ >= static_cast<int64_t>(disk_->FileSize()))) {
            root_offset_      = INVALID_PAGE_ID;
            next_page_offset_ = PAGE_SIZE;
        }
    }
}

// ============================================================================
// Page access helpers (through buffer pool)
// ============================================================================

char* BPlusTree::PinPage(int64_t page_id) const {
    return pool_->FetchPage(page_id);
}

void BPlusTree::UnpinPage(int64_t page_id, bool dirty) const {
    pool_->UnpinPage(page_id, dirty);
}

char* BPlusTree::AllocPage(int64_t& page_id) {
    char* data = pool_->NewPage(page_id);
    next_page_offset_ = disk_->NextPageOffset();
    return data;
}

void BPlusTree::DeallocPage(int64_t page_id) {
    pool_->DeletePage(page_id);
    disk_->FreePage(page_id);
}

// ============================================================================
// Utilities
// ============================================================================

bool BPlusTree::IsEmpty() const { return root_offset_ == INVALID_PAGE_ID; }

void BPlusTree::Sync() { pool_->FlushAllPages(); }

std::string BPlusTree::FilePath() const { return disk_->FilePath(); }

size_t BPlusTree::BufferPoolHits()   const { return pool_->HitCount(); }
size_t BPlusTree::BufferPoolMisses() const { return pool_->MissCount(); }
double BPlusTree::BufferPoolHitRate() const { return pool_->HitRate(); }

size_t BPlusTree::WALBytesWritten()   const { return wal_ ? wal_->BytesWritten() : 0; }
size_t BPlusTree::WALRecordsWritten() const { return wal_ ? wal_->RecordsWritten() : 0; }
bool   BPlusTree::WALEnabled()        const { return wal_ != nullptr; }

void BPlusTree::Checkpoint() {
    if (!wal_) return;
    wal_->BeginCheckpoint();
    pool_->FlushAllPages();
    wal_->EndCheckpoint();
}

// ============================================================================
// Search
// ============================================================================

int64_t BPlusTree::SearchLeaf(key_t key) const {
    if (root_offset_ == INVALID_PAGE_ID) return INVALID_PAGE_ID;

    int64_t current = root_offset_;
    char* page = PinPage(current);
    if (!page) return INVALID_PAGE_ID;

    while (!PageIsLeaf(page)) {
        InternalPage node(page);
        int n = node.NumKeys();
        int i = 0;
        while (i < n && key >= node.KeyAt(i)) ++i;
        int64_t child = node.ChildAt(i);
        UnpinPage(current, false);

        current = child;
        if (current < static_cast<int64_t>(PAGE_SIZE)) return INVALID_PAGE_ID;

        page = PinPage(current);
        if (!page) return INVALID_PAGE_ID;
    }

    // Return with the leaf still pinned -- caller must unpin.
    UnpinPage(current, false);
    return current;
}

Status BPlusTree::Search(key_t key, char* data_out) const {
    int64_t leaf_off = SearchLeaf(key);
    if (leaf_off == INVALID_PAGE_ID) return Status::NotFound("key not found");

    char* page = PinPage(leaf_off);
    if (!page) return Status::IOError("cannot pin page");

    LeafPage leaf(page);
    int n = leaf.NumKeys();
    for (int i = 0; i < n; ++i) {
        if (leaf.KeyAt(i) == key) {
            leaf.GetData(i, data_out);
            UnpinPage(leaf_off, false);
            return Status::OK();
        }
    }
    UnpinPage(leaf_off, false);
    return Status::NotFound("key not found");
}

Status BPlusTree::Search(key_t key, std::string& value_out) const {
    char buf[DATA_SIZE];
    Status s = Search(key, buf);
    if (s.ok()) {
        size_t len = ::strnlen(buf, DATA_SIZE);
        value_out.assign(buf, len);
    }
    return s;
}

// ============================================================================
// Range query
// ============================================================================

Status BPlusTree::RangeQuery(key_t lower, key_t upper,
                             std::vector<std::pair<key_t, std::string>>& results) const {
    results.clear();

    if (lower > upper) return Status::InvalidArg("lower > upper");
    if (root_offset_ == INVALID_PAGE_ID) return Status::OK();

    int64_t leaf_off = SearchLeaf(lower);
    if (leaf_off == INVALID_PAGE_ID) return Status::OK();

    while (leaf_off != INVALID_PAGE_ID && leaf_off >= static_cast<int64_t>(PAGE_SIZE)) {
        char* page = PinPage(leaf_off);
        if (!page) break;

        LeafPage leaf(page);
        int n = leaf.NumKeys();

        bool done = false;
        for (int i = 0; i < n; ++i) {
            int k = leaf.KeyAt(i);
            if (k > upper) { done = true; break; }
            if (k >= lower) {
                char buf[DATA_SIZE];
                leaf.GetData(i, buf);
                results.emplace_back(k, std::string(buf, ::strnlen(buf, DATA_SIZE)));
            }
        }

        int64_t next = leaf.NextLeaf();
        UnpinPage(leaf_off, false);

        if (done) break;
        leaf_off = next;
    }

    return Status::OK();
}

// ============================================================================
// Insert
// ============================================================================

Status BPlusTree::Insert(key_t key, const char* data) {
    char padded[DATA_SIZE]{};
    std::memcpy(padded, data, std::min(std::strlen(data) + 1, DATA_SIZE));

    // Empty tree -- create root leaf.
    if (root_offset_ == INVALID_PAGE_ID) {
        int64_t off;
        char* page = AllocPage(off);
        if (!page) return Status::IOError("cannot allocate page");

        LeafPage::Init(page);
        LeafPage leaf(page);
        leaf.SetNumKeys(1);
        leaf.SetRecord(0, key, padded);

        UnpinPage(off, true);
        root_offset_ = off;
        WriteMetadata();
        return Status::OK();
    }

    key_t   split_key;
    int64_t new_off;
    bool split = InsertRecursive(root_offset_, key, padded, split_key, new_off);

    if (split) {
        int64_t new_root;
        char* page = AllocPage(new_root);
        if (!page) return Status::IOError("cannot allocate page");

        InternalPage::Init(page);
        InternalPage root(page);
        root.SetNumKeys(1);
        root.SetKeyAt(0, split_key);
        root.SetChildAt(0, root_offset_);
        root.SetChildAt(1, new_off);

        UnpinPage(new_root, true);
        root_offset_ = new_root;
        WriteMetadata();
    }

    return Status::OK();
}

bool BPlusTree::InsertRecursive(int64_t node_off, key_t key, const char* data,
                                key_t& split_key, int64_t& new_off) {
    char* page = PinPage(node_off);

    if (PageIsLeaf(page)) {
        UnpinPage(node_off, false);
        return InsertIntoLeaf(node_off, key, data, split_key, new_off);
    }

    InternalPage node(page);
    int n = node.NumKeys();
    int i = 0;
    while (i < n && key >= node.KeyAt(i)) ++i;
    int64_t child = node.ChildAt(i);
    UnpinPage(node_off, false);

    key_t   child_split;
    int64_t child_new;
    bool child_did_split = InsertRecursive(child, key, data,
                                           child_split, child_new);
    if (!child_did_split) return false;

    return InsertIntoInternal(node_off, child_split, child_new, split_key, new_off);
}

bool BPlusTree::InsertIntoLeaf(int64_t leaf_off, key_t key, const char* data,
                               key_t& split_key, int64_t& new_leaf_off) {
    char* page = PinPage(leaf_off);
    LeafPage leaf(page);
    int n = leaf.NumKeys();

    // Existing key -- update.
    for (int i = 0; i < n; ++i) {
        if (leaf.KeyAt(i) == key) {
            leaf.SetData(i, data);
            UnpinPage(leaf_off, true);
            return false;
        }
    }

    // Room available.
    if (n < LEAF_MAX_KEYS) {
        int i = n - 1;
        while (i >= 0 && leaf.KeyAt(i) > key) {
            int tk; char td[DATA_SIZE];
            leaf.GetRecord(i, tk, td);
            leaf.SetRecord(i + 1, tk, td);
            --i;
        }
        leaf.SetRecord(i + 1, key, data);
        leaf.SetNumKeys(n + 1);
        UnpinPage(leaf_off, true);
        return false;
    }

    // Full -- split.
    struct Rec { int k; char d[DATA_SIZE]; };
    std::vector<Rec> recs(n);
    for (int i = 0; i < n; ++i) leaf.GetRecord(i, recs[i].k, recs[i].d);

    Rec nr; nr.k = key; std::memcpy(nr.d, data, DATA_SIZE);
    auto it = std::lower_bound(recs.begin(), recs.end(), nr,
                               [](const Rec& a, const Rec& b){ return a.k < b.k; });
    recs.insert(it, nr);

    int mid = static_cast<int>((recs.size() + 1) / 2);

    // New leaf.
    char* new_page = AllocPage(new_leaf_off);
    LeafPage::Init(new_page);
    LeafPage new_leaf(new_page);
    new_leaf.SetNumKeys(static_cast<int>(recs.size()) - mid);
    for (int i = mid; i < static_cast<int>(recs.size()); ++i) {
        new_leaf.SetRecord(i - mid, recs[i].k, recs[i].d);
    }

    // Linked list.
    new_leaf.SetNextLeaf(leaf.NextLeaf());
    UnpinPage(new_leaf_off, true);

    // Left half stays in the original page (still pinned from above).
    leaf.SetNumKeys(mid);
    for (int i = 0; i < mid; ++i) leaf.SetRecord(i, recs[i].k, recs[i].d);
    leaf.SetNextLeaf(new_leaf_off);
    UnpinPage(leaf_off, true);

    // Re-read split key.
    char* npage = PinPage(new_leaf_off);
    split_key = LeafPage(npage).KeyAt(0);
    UnpinPage(new_leaf_off, false);

    return true;
}

bool BPlusTree::InsertIntoInternal(int64_t node_off, key_t key, int64_t child_off,
                                   key_t& split_key, int64_t& new_node_off) {
    char* page = PinPage(node_off);
    InternalPage node(page);
    int n = node.NumKeys();

    // Room available.
    if (n < INTERNAL_MAX_KEYS) {
        int i = n - 1;
        while (i >= 0 && node.KeyAt(i) > key) {
            node.SetKeyAt(i + 1, node.KeyAt(i));
            node.SetChildAt(i + 2, node.ChildAt(i + 1));
            --i;
        }
        node.SetKeyAt(i + 1, key);
        node.SetChildAt(i + 2, child_off);
        node.SetNumKeys(n + 1);
        UnpinPage(node_off, true);
        return false;
    }

    // Full -- split.
    std::vector<int>     keys(n);
    std::vector<int64_t> children(n + 1);
    for (int i = 0; i < n; ++i) keys[i] = node.KeyAt(i);
    for (int i = 0; i <= n; ++i) children[i] = node.ChildAt(i);
    UnpinPage(node_off, false);

    int pos = 0;
    while (pos < static_cast<int>(keys.size()) && keys[pos] < key) ++pos;
    keys.insert(keys.begin() + pos, key);
    children.insert(children.begin() + pos + 1, child_off);

    int mid = static_cast<int>(keys.size()) / 2;
    split_key = keys[mid];

    // New internal node.
    char* new_page = AllocPage(new_node_off);
    InternalPage::Init(new_page);
    InternalPage new_node(new_page);
    int right_count = static_cast<int>(keys.size()) - mid - 1;
    new_node.SetNumKeys(right_count);
    for (int j = mid + 1; j < static_cast<int>(keys.size()); ++j) {
        new_node.SetKeyAt(j - mid - 1, keys[j]);
    }
    for (int j = mid + 1; j < static_cast<int>(children.size()); ++j) {
        new_node.SetChildAt(j - mid - 1, children[j]);
    }
    UnpinPage(new_node_off, true);

    // Write left half.
    page = PinPage(node_off);
    node = InternalPage(page);
    node.SetNumKeys(mid);
    for (int j = 0; j < mid; ++j) {
        node.SetKeyAt(j, keys[j]);
        node.SetChildAt(j, children[j]);
    }
    node.SetChildAt(mid, children[mid]);
    UnpinPage(node_off, true);

    return true;
}

// ============================================================================
// Delete (with rebalancing)
// ============================================================================

Status BPlusTree::Delete(key_t key) {
    if (root_offset_ == INVALID_PAGE_ID) return Status::NotFound("key not found");

    // Check existence first so we can return NotFound properly.
    {
        char buf[DATA_SIZE];
        Status s = Search(key, buf);
        if (!s.ok()) return Status::NotFound("key not found");
    }

    bool underful = DeleteRecursive(root_offset_, key);

    if (underful) {
        // Check if root is an empty internal node -- shrink the tree.
        char* page = PinPage(root_offset_);
        if (!PageIsLeaf(page)) {
            InternalPage root(page);
            if (root.NumKeys() == 0) {
                int64_t old_root = root_offset_;
                root_offset_ = root.ChildAt(0);
                UnpinPage(old_root, false);
                DeallocPage(old_root);
                WriteMetadata();
                return Status::OK();
            }
        } else {
            LeafPage root(page);
            if (root.NumKeys() == 0) {
                int64_t old_root = root_offset_;
                root_offset_ = INVALID_PAGE_ID;
                UnpinPage(old_root, false);
                DeallocPage(old_root);
                WriteMetadata();
                return Status::OK();
            }
        }
        UnpinPage(root_offset_, false);
    }

    return Status::OK();
}

bool BPlusTree::DeleteRecursive(int64_t node_off, key_t key) {
    char* page = PinPage(node_off);

    if (PageIsLeaf(page)) {
        UnpinPage(node_off, false);
        return DeleteFromLeaf(node_off, key);
    }

    // Internal node -- find the child.
    InternalPage node(page);
    int n = node.NumKeys();
    int i = 0;
    while (i < n && key >= node.KeyAt(i)) ++i;
    int64_t child = node.ChildAt(i);
    UnpinPage(node_off, false);

    bool child_underful = DeleteRecursive(child, key);

    if (child_underful) {
        FixChild(node_off, i);

        // Check if this node is now underful.
        page = PinPage(node_off);
        InternalPage n2(page);
        int nk = n2.NumKeys();
        UnpinPage(node_off, false);

        // Root is allowed to have fewer keys.
        if (node_off == root_offset_) return (nk == 0);
        return (nk < INTERNAL_MIN_KEYS);
    }

    return false;
}

bool BPlusTree::DeleteFromLeaf(int64_t leaf_off, key_t key) {
    char* page = PinPage(leaf_off);
    LeafPage leaf(page);
    int n = leaf.NumKeys();

    int found = -1;
    for (int i = 0; i < n; ++i) {
        if (leaf.KeyAt(i) == key) { found = i; break; }
    }

    if (found == -1) {
        UnpinPage(leaf_off, false);
        // Propagate NotFound via a non-underful return so the caller
        // returns OK only for actual deletes.  We handle it simply by
        // not modifying anything and returning false (not underful).
        return false;
    }

    // Shift remaining records left.
    for (int j = found; j < n - 1; ++j) {
        int tk; char td[DATA_SIZE];
        leaf.GetRecord(j + 1, tk, td);
        leaf.SetRecord(j, tk, td);
    }
    leaf.SetNumKeys(n - 1);
    UnpinPage(leaf_off, true);

    // Is this leaf underful?
    if (leaf_off == root_offset_) return (n - 1 == 0);
    return (n - 1 < LEAF_MIN_KEYS);
}

// ============================================================================
// Rebalancing
// ============================================================================

void BPlusTree::FixChild(int64_t parent_off, int child_idx) {
    char* ppage = PinPage(parent_off);
    InternalPage parent(ppage);
    int64_t child_off = parent.ChildAt(child_idx);
    UnpinPage(parent_off, false);

    char* cpage = PinPage(child_off);
    bool child_is_leaf = PageIsLeaf(cpage);
    UnpinPage(child_off, false);

    if (child_is_leaf) {
        FixLeafChild(parent_off, child_idx);
    } else {
        FixInternalChild(parent_off, child_idx);
    }
}

void BPlusTree::FixLeafChild(int64_t parent_off, int child_idx) {
    char* ppage = PinPage(parent_off);
    InternalPage parent(ppage);
    int parent_keys = parent.NumKeys();
    int64_t child_off = parent.ChildAt(child_idx);

    // Try to borrow from left sibling.
    if (child_idx > 0) {
        int64_t left_off = parent.ChildAt(child_idx - 1);
        UnpinPage(parent_off, false);

        char* lpage = PinPage(left_off);
        LeafPage left(lpage);
        int left_n = left.NumKeys();

        if (left_n > LEAF_MIN_KEYS) {
            // Borrow the last record from left sibling.
            int tk; char td[DATA_SIZE];
            left.GetRecord(left_n - 1, tk, td);
            left.SetNumKeys(left_n - 1);
            UnpinPage(left_off, true);

            // Insert at the front of child.
            char* cpage = PinPage(child_off);
            LeafPage child(cpage);
            int cn = child.NumKeys();
            // Shift right.
            for (int j = cn - 1; j >= 0; --j) {
                int k2; char d2[DATA_SIZE];
                child.GetRecord(j, k2, d2);
                child.SetRecord(j + 1, k2, d2);
            }
            child.SetRecord(0, tk, td);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            // Update parent key.
            ppage = PinPage(parent_off);
            InternalPage p2(ppage);
            p2.SetKeyAt(child_idx - 1, tk);
            UnpinPage(parent_off, true);
            return;
        }
        UnpinPage(left_off, false);
    } else {
        UnpinPage(parent_off, false);
    }

    // Try to borrow from right sibling.
    ppage = PinPage(parent_off);
    parent = InternalPage(ppage);
    if (child_idx < parent_keys) {
        int64_t right_off = parent.ChildAt(child_idx + 1);
        UnpinPage(parent_off, false);

        char* rpage = PinPage(right_off);
        LeafPage right(rpage);
        int right_n = right.NumKeys();

        if (right_n > LEAF_MIN_KEYS) {
            // Borrow the first record from right sibling.
            int tk; char td[DATA_SIZE];
            right.GetRecord(0, tk, td);
            // Shift left in right.
            for (int j = 0; j < right_n - 1; ++j) {
                int k2; char d2[DATA_SIZE];
                right.GetRecord(j + 1, k2, d2);
                right.SetRecord(j, k2, d2);
            }
            right.SetNumKeys(right_n - 1);
            // New first key of right.
            int new_right_first = right.KeyAt(0);
            UnpinPage(right_off, true);

            // Append to child.
            char* cpage = PinPage(child_off);
            LeafPage child(cpage);
            int cn = child.NumKeys();
            child.SetRecord(cn, tk, td);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            // Update parent key.
            ppage = PinPage(parent_off);
            InternalPage p2(ppage);
            p2.SetKeyAt(child_idx, new_right_first);
            UnpinPage(parent_off, true);
            return;
        }
        UnpinPage(right_off, false);
    } else {
        UnpinPage(parent_off, false);
    }

    // Cannot borrow -- merge.
    // Always merge child into its left sibling if possible, otherwise
    // merge right sibling into child.
    ppage = PinPage(parent_off);
    parent = InternalPage(ppage);

    int64_t left_off, right_off;
    int merge_key_idx;

    if (child_idx > 0) {
        // Merge child into left sibling.
        left_off = parent.ChildAt(child_idx - 1);
        right_off = child_off;
        merge_key_idx = child_idx - 1;
    } else {
        // Merge right sibling into child.
        left_off = child_off;
        right_off = parent.ChildAt(child_idx + 1);
        merge_key_idx = child_idx;
    }
    UnpinPage(parent_off, false);

    // Copy all records from right into left.
    char* lpage = PinPage(left_off);
    LeafPage left(lpage);
    int ln = left.NumKeys();

    char* rpage = PinPage(right_off);
    LeafPage right(rpage);
    int rn = right.NumKeys();

    for (int j = 0; j < rn; ++j) {
        int tk; char td[DATA_SIZE];
        right.GetRecord(j, tk, td);
        left.SetRecord(ln + j, tk, td);
    }
    left.SetNumKeys(ln + rn);
    left.SetNextLeaf(right.NextLeaf());

    UnpinPage(left_off, true);
    UnpinPage(right_off, false);
    DeallocPage(right_off);

    // Remove merge_key_idx from parent.
    ppage = PinPage(parent_off);
    parent = InternalPage(ppage);
    int pn = parent.NumKeys();
    for (int j = merge_key_idx; j < pn - 1; ++j) {
        parent.SetKeyAt(j, parent.KeyAt(j + 1));
        parent.SetChildAt(j + 1, parent.ChildAt(j + 2));
    }
    parent.SetNumKeys(pn - 1);
    UnpinPage(parent_off, true);
}

void BPlusTree::FixInternalChild(int64_t parent_off, int child_idx) {
    char* ppage = PinPage(parent_off);
    InternalPage parent(ppage);
    int parent_keys = parent.NumKeys();
    int64_t child_off = parent.ChildAt(child_idx);

    // Try to borrow from left sibling.
    if (child_idx > 0) {
        int64_t left_off = parent.ChildAt(child_idx - 1);
        int parent_key = parent.KeyAt(child_idx - 1);
        UnpinPage(parent_off, false);

        char* lpage = PinPage(left_off);
        InternalPage left(lpage);
        int left_n = left.NumKeys();

        if (left_n > INTERNAL_MIN_KEYS) {
            // Borrow: take the last key from left, push parent key down to child.
            int borrowed_key = left.KeyAt(left_n - 1);
            int64_t borrowed_child = left.ChildAt(left_n);
            left.SetNumKeys(left_n - 1);
            UnpinPage(left_off, true);

            // Prepend parent_key + borrowed_child to child.
            char* cpage = PinPage(child_off);
            InternalPage child(cpage);
            int cn = child.NumKeys();
            // Shift right.
            for (int j = cn - 1; j >= 0; --j) {
                child.SetKeyAt(j + 1, child.KeyAt(j));
                child.SetChildAt(j + 2, child.ChildAt(j + 1));
            }
            child.SetChildAt(1, child.ChildAt(0));
            child.SetKeyAt(0, parent_key);
            child.SetChildAt(0, borrowed_child);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            // Replace parent key with borrowed key.
            ppage = PinPage(parent_off);
            InternalPage p2(ppage);
            p2.SetKeyAt(child_idx - 1, borrowed_key);
            UnpinPage(parent_off, true);
            return;
        }
        UnpinPage(left_off, false);
    } else {
        UnpinPage(parent_off, false);
    }

    // Try to borrow from right sibling.
    ppage = PinPage(parent_off);
    parent = InternalPage(ppage);
    if (child_idx < parent_keys) {
        int64_t right_off = parent.ChildAt(child_idx + 1);
        int parent_key = parent.KeyAt(child_idx);
        UnpinPage(parent_off, false);

        char* rpage = PinPage(right_off);
        InternalPage right(rpage);
        int right_n = right.NumKeys();

        if (right_n > INTERNAL_MIN_KEYS) {
            int borrowed_key = right.KeyAt(0);
            int64_t borrowed_child = right.ChildAt(0);
            // Shift left in right.
            for (int j = 0; j < right_n - 1; ++j) {
                right.SetKeyAt(j, right.KeyAt(j + 1));
                right.SetChildAt(j, right.ChildAt(j + 1));
            }
            right.SetChildAt(right_n - 1, right.ChildAt(right_n));
            right.SetNumKeys(right_n - 1);
            UnpinPage(right_off, true);

            // Append parent_key + borrowed_child to child.
            char* cpage = PinPage(child_off);
            InternalPage child(cpage);
            int cn = child.NumKeys();
            child.SetKeyAt(cn, parent_key);
            child.SetChildAt(cn + 1, borrowed_child);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            // Replace parent key with borrowed key.
            ppage = PinPage(parent_off);
            InternalPage p2(ppage);
            p2.SetKeyAt(child_idx, borrowed_key);
            UnpinPage(parent_off, true);
            return;
        }
        UnpinPage(right_off, false);
    } else {
        UnpinPage(parent_off, false);
    }

    // Cannot borrow -- merge.
    ppage = PinPage(parent_off);
    parent = InternalPage(ppage);

    int64_t left_off, right_off;
    int merge_key_idx;

    if (child_idx > 0) {
        left_off = parent.ChildAt(child_idx - 1);
        right_off = child_off;
        merge_key_idx = child_idx - 1;
    } else {
        left_off = child_off;
        right_off = parent.ChildAt(child_idx + 1);
        merge_key_idx = child_idx;
    }
    int merge_key = parent.KeyAt(merge_key_idx);
    UnpinPage(parent_off, false);

    // Merge: left + merge_key + right -> left.
    char* lpage = PinPage(left_off);
    InternalPage left(lpage);
    int ln = left.NumKeys();

    char* rpage = PinPage(right_off);
    InternalPage right(rpage);
    int rn = right.NumKeys();

    // Append merge_key.
    left.SetKeyAt(ln, merge_key);
    left.SetChildAt(ln + 1, right.ChildAt(0));

    // Copy right's keys and children.
    for (int j = 0; j < rn; ++j) {
        left.SetKeyAt(ln + 1 + j, right.KeyAt(j));
        left.SetChildAt(ln + 2 + j, right.ChildAt(j + 1));
    }
    left.SetNumKeys(ln + 1 + rn);

    UnpinPage(left_off, true);
    UnpinPage(right_off, false);
    DeallocPage(right_off);

    // Remove merge_key_idx from parent.
    ppage = PinPage(parent_off);
    parent = InternalPage(ppage);
    int pn = parent.NumKeys();
    for (int j = merge_key_idx; j < pn - 1; ++j) {
        parent.SetKeyAt(j, parent.KeyAt(j + 1));
        parent.SetChildAt(j + 1, parent.ChildAt(j + 2));
    }
    parent.SetNumKeys(pn - 1);
    UnpinPage(parent_off, true);
}

}  // namespace bptree
