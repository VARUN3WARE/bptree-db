/// @file bplus_tree.tpp
/// @brief Template implementation of BPlusTree<K, Cmp>.
///
/// This file is included at the bottom of bplus_tree.h (the ".tpp pattern")
/// so the compiler sees the full definition whenever it instantiates the
/// template.  Do not compile this file directly. :)

#pragma once

#include "bptree/page.h"
#include "bptree/wal.h"
#include "bptree/disk_manager.h"
#include "bptree/buffer_pool.h"

#include <algorithm>
#include <cstring>
#include <vector>

namespace bptree {

// ============================================================================
// Construction / destruction
// ============================================================================

template <typename K, typename Cmp>
BPlusTree<K,Cmp>::BPlusTree(const std::string& index_file,
                             size_t pool_size, bool enable_wal)
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

template <typename K, typename Cmp>
BPlusTree<K,Cmp>::~BPlusTree() {
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

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::WriteMetadata() {
    disk_->SetRootOffset(root_offset_);
    disk_->SetNextPageOffset(next_page_offset_);
    disk_->FlushMetadata();
}

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::ReadMetadata() {
    if (disk_->FileSize() >= PAGE_SIZE) {
        root_offset_      = disk_->RootOffset();
        next_page_offset_ = disk_->NextPageOffset();

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

template <typename K, typename Cmp>
char* BPlusTree<K,Cmp>::PinPage(int64_t page_id) const {
    return pool_->FetchPage(page_id);
}

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::UnpinPage(int64_t page_id, bool dirty) const {
    pool_->UnpinPage(page_id, dirty);
}

template <typename K, typename Cmp>
char* BPlusTree<K,Cmp>::AllocPage(int64_t& page_id) {
    char* data = pool_->NewPage(page_id);
    next_page_offset_ = disk_->NextPageOffset();
    return data;
}

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::DeallocPage(int64_t page_id) {
    pool_->DeletePage(page_id);
    disk_->FreePage(page_id);
}

// ============================================================================
// Utilities and latch helpers
// ============================================================================

template <typename K, typename Cmp>
bool BPlusTree<K,Cmp>::IsEmpty() const { return root_offset_ == INVALID_PAGE_ID; }

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::Sync() { pool_->FlushAllPages(); }

template <typename K, typename Cmp>
std::string BPlusTree<K,Cmp>::FilePath() const { return disk_->FilePath(); }

template <typename K, typename Cmp>
size_t BPlusTree<K,Cmp>::BufferPoolHits()   const { return pool_->HitCount(); }

template <typename K, typename Cmp>
size_t BPlusTree<K,Cmp>::BufferPoolMisses() const { return pool_->MissCount(); }

template <typename K, typename Cmp>
double BPlusTree<K,Cmp>::BufferPoolHitRate() const { return pool_->HitRate(); }

template <typename K, typename Cmp>
size_t BPlusTree<K,Cmp>::WALBytesWritten()   const { return wal_ ? wal_->BytesWritten()   : 0; }

template <typename K, typename Cmp>
size_t BPlusTree<K,Cmp>::WALRecordsWritten() const { return wal_ ? wal_->RecordsWritten() : 0; }

template <typename K, typename Cmp>
bool BPlusTree<K,Cmp>::WALEnabled() const { return wal_ != nullptr; }

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::Checkpoint() {
    if (!wal_) return;
    wal_->BeginCheckpoint();
    pool_->FlushAllPages();
    wal_->EndCheckpoint();
}

// Latch helpers -- just delegate to the pool.
template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::RLatch(int64_t page_id)   const { pool_->RLatchPage(page_id); }

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::RUnlatch(int64_t page_id) const { pool_->RUnlatchPage(page_id); }

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::WLatch(int64_t page_id)   const { pool_->WLatchPage(page_id); }

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::WUnlatch(int64_t page_id) const { pool_->WUnlatchPage(page_id); }

// ============================================================================
// Search
// ============================================================================

// SearchLeaf uses shared (read) latches -- crab-walk: acquire child,
// release parent.  Multiple readers traverse simultaneously. :)
template <typename K, typename Cmp>
int64_t BPlusTree<K,Cmp>::SearchLeaf(const K& key) const {
    if (root_offset_ == INVALID_PAGE_ID) return INVALID_PAGE_ID;

    int64_t current = root_offset_;
    char* page = PinPage(current);
    if (!page) return INVALID_PAGE_ID;
    RLatch(current);

    while (!PageIsLeaf(page)) {
        InternalPage<K> node(page);
        int n = node.NumKeys();
        int i = 0;
        while (i < n && GE(key, node.KeyAt(i))) ++i;
        int64_t child = node.ChildAt(i);

        char* child_page = PinPage(child);
        if (!child_page) {
            RUnlatch(current);
            UnpinPage(current, false);
            return INVALID_PAGE_ID;
        }
        RLatch(child);

        RUnlatch(current);
        UnpinPage(current, false);

        current = child;
        page    = child_page;

        if (current < static_cast<int64_t>(PAGE_SIZE)) {
            RUnlatch(current);
            UnpinPage(current, false);
            return INVALID_PAGE_ID;
        }
    }

    RUnlatch(current);
    UnpinPage(current, false);
    return current;
}

template <typename K, typename Cmp>
Status BPlusTree<K,Cmp>::Search(const K& key, char* data_out) const {
    int64_t leaf_off = SearchLeaf(key);
    if (leaf_off == INVALID_PAGE_ID) return Status::NotFound("key not found");

    char* page = PinPage(leaf_off);
    if (!page) return Status::IOError("cannot pin page");

    LeafPage<K> leaf(page);
    int n = leaf.NumKeys();
    for (int i = 0; i < n; ++i) {
        if (Equal(leaf.KeyAt(i), key)) {
            leaf.GetData(i, data_out);
            UnpinPage(leaf_off, false);
            return Status::OK();
        }
    }
    UnpinPage(leaf_off, false);
    return Status::NotFound("key not found");
}

template <typename K, typename Cmp>
Status BPlusTree<K,Cmp>::Search(const K& key, std::string& value_out) const {
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

template <typename K, typename Cmp>
Status BPlusTree<K,Cmp>::RangeQuery(const K& lower, const K& upper,
                                    std::vector<std::pair<K, std::string>>& results) const {
    results.clear();

    if (Less(upper, lower)) return Status::InvalidArg("lower > upper");
    if (root_offset_ == INVALID_PAGE_ID) return Status::OK();

    int64_t leaf_off = SearchLeaf(lower);
    if (leaf_off == INVALID_PAGE_ID) return Status::OK();

    while (leaf_off != INVALID_PAGE_ID && leaf_off >= static_cast<int64_t>(PAGE_SIZE)) {
        char* page = PinPage(leaf_off);
        if (!page) break;

        LeafPage<K> leaf(page);
        int n = leaf.NumKeys();

        bool done = false;
        for (int i = 0; i < n; ++i) {
            K k = leaf.KeyAt(i);
            if (Less(upper, k)) { done = true; break; }  // k > upper
            if (!Less(k, lower)) {                         // k >= lower
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

template <typename K, typename Cmp>
Status BPlusTree<K,Cmp>::Insert(const K& key, const char* data) {
    std::lock_guard<std::mutex> lock(tree_mtx_);

    char padded[DATA_SIZE]{};
    std::memcpy(padded, data, std::min(std::strlen(data) + 1, DATA_SIZE));

    // Empty tree -- create root leaf.
    if (root_offset_ == INVALID_PAGE_ID) {
        int64_t off;
        char* page = AllocPage(off);
        if (!page) return Status::IOError("cannot allocate page");

        LeafPage<K>::Init(page);
        LeafPage<K> leaf(page);
        leaf.SetNumKeys(1);
        leaf.SetRecord(0, key, padded);

        UnpinPage(off, true);
        root_offset_ = off;
        WriteMetadata();
        return Status::OK();
    }

    K       split_key{};
    int64_t new_off;
    bool split = InsertRecursive(root_offset_, key, padded, split_key, new_off);

    if (split) {
        int64_t new_root;
        char* page = AllocPage(new_root);
        if (!page) return Status::IOError("cannot allocate page");

        InternalPage<K>::Init(page);
        InternalPage<K> root(page);
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

// InsertRecursive uses optimistic write-latch crabbing: acquire write latch,
// release early if the node cannot cause a split. :)
template <typename K, typename Cmp>
bool BPlusTree<K,Cmp>::InsertRecursive(int64_t node_off, const K& key, const char* data,
                                        K& split_key, int64_t& new_off) {
    char* page = PinPage(node_off);
    WLatch(node_off);

    if (PageIsLeaf(page)) {
        WUnlatch(node_off);
        UnpinPage(node_off, false);
        return InsertIntoLeaf(node_off, key, data, split_key, new_off);
    }

    InternalPage<K> node(page);
    int n = node.NumKeys();
    int i = 0;
    while (i < n && GE(key, node.KeyAt(i))) ++i;
    int64_t child = node.ChildAt(i);

    WUnlatch(node_off);
    UnpinPage(node_off, false);

    K       child_split{};
    int64_t child_new;
    bool child_did_split = InsertRecursive(child, key, data, child_split, child_new);
    if (!child_did_split) return false;

    return InsertIntoInternal(node_off, child_split, child_new, split_key, new_off);
}

template <typename K, typename Cmp>
bool BPlusTree<K,Cmp>::InsertIntoLeaf(int64_t leaf_off, const K& key, const char* data,
                                       K& split_key, int64_t& new_leaf_off) {
    char* page = PinPage(leaf_off);
    WLatch(leaf_off);
    LeafPage<K> leaf(page);
    int n = leaf.NumKeys();

    // Existing key -- update (upsert).
    for (int i = 0; i < n; ++i) {
        if (Equal(leaf.KeyAt(i), key)) {
            leaf.SetData(i, data);
            WUnlatch(leaf_off);
            UnpinPage(leaf_off, true);
            return false;
        }
    }

    // Room available.
    if (n < LeafPage<K>::kMaxKeys) {
        int i = n - 1;
        while (i >= 0 && Less(key, leaf.KeyAt(i))) {
            K tk{}; char td[DATA_SIZE];
            leaf.GetRecord(i, tk, td);
            leaf.SetRecord(i + 1, tk, td);
            --i;
        }
        leaf.SetRecord(i + 1, key, data);
        leaf.SetNumKeys(n + 1);
        WUnlatch(leaf_off);
        UnpinPage(leaf_off, true);
        return false;
    }

    // Full -- split.  Copy records out, release latch before AllocPage.
    struct Rec { K k; char d[DATA_SIZE]; };
    std::vector<Rec> recs(n);
    for (int i = 0; i < n; ++i) {
        recs[i].k = leaf.KeyAt(i);
        leaf.GetData(i, recs[i].d);
    }
    int64_t old_next = leaf.NextLeaf();
    WUnlatch(leaf_off);
    UnpinPage(leaf_off, false);

    Rec nr; nr.k = key; std::memcpy(nr.d, data, DATA_SIZE);
    auto it = std::lower_bound(recs.begin(), recs.end(), nr,
                               [this](const Rec& a, const Rec& b){ return Less(a.k, b.k); });
    recs.insert(it, nr);

    int mid = static_cast<int>((recs.size() + 1) / 2);

    // New leaf.
    char* new_page = AllocPage(new_leaf_off);
    WLatch(new_leaf_off);
    LeafPage<K>::Init(new_page);
    LeafPage<K> new_leaf(new_page);
    new_leaf.SetNumKeys(static_cast<int>(recs.size()) - mid);
    for (int i = mid; i < static_cast<int>(recs.size()); ++i) {
        new_leaf.SetRecord(i - mid, recs[i].k, recs[i].d);
    }
    new_leaf.SetNextLeaf(old_next);
    WUnlatch(new_leaf_off);
    UnpinPage(new_leaf_off, true);

    // Write left half back.
    page = PinPage(leaf_off);
    WLatch(leaf_off);
    LeafPage<K> leaf2(page);
    leaf2.SetNumKeys(mid);
    for (int i = 0; i < mid; ++i) leaf2.SetRecord(i, recs[i].k, recs[i].d);
    leaf2.SetNextLeaf(new_leaf_off);
    WUnlatch(leaf_off);
    UnpinPage(leaf_off, true);

    // Re-read split key.
    char* npage = PinPage(new_leaf_off);
    split_key = LeafPage<K>(npage).KeyAt(0);
    UnpinPage(new_leaf_off, false);

    return true;
}

template <typename K, typename Cmp>
bool BPlusTree<K,Cmp>::InsertIntoInternal(int64_t node_off, const K& key, int64_t child_off,
                                           K& split_key, int64_t& new_node_off) {
    char* page = PinPage(node_off);
    WLatch(node_off);
    InternalPage<K> node(page);
    int n = node.NumKeys();

    // Room available.
    if (n < InternalPage<K>::kMaxKeys) {
        int i = n - 1;
        while (i >= 0 && Less(key, node.KeyAt(i))) {
            node.SetKeyAt(i + 1, node.KeyAt(i));
            node.SetChildAt(i + 2, node.ChildAt(i + 1));
            --i;
        }
        node.SetKeyAt(i + 1, key);
        node.SetChildAt(i + 2, child_off);
        node.SetNumKeys(n + 1);
        WUnlatch(node_off);
        UnpinPage(node_off, true);
        return false;
    }

    // Full -- copy out and release before AllocPage.
    std::vector<K>       keys(n);
    std::vector<int64_t> children(n + 1);
    for (int i = 0; i < n; ++i) keys[i] = node.KeyAt(i);
    for (int i = 0; i <= n; ++i) children[i] = node.ChildAt(i);
    WUnlatch(node_off);
    UnpinPage(node_off, false);

    int pos = 0;
    while (pos < static_cast<int>(keys.size()) && Less(keys[pos], key)) ++pos;
    keys.insert(keys.begin() + pos, key);
    children.insert(children.begin() + pos + 1, child_off);

    int mid = static_cast<int>(keys.size()) / 2;
    split_key = keys[mid];

    // New internal node.
    char* new_page = AllocPage(new_node_off);
    WLatch(new_node_off);
    InternalPage<K>::Init(new_page);
    InternalPage<K> new_node(new_page);
    int right_count = static_cast<int>(keys.size()) - mid - 1;
    new_node.SetNumKeys(right_count);
    for (int j = mid + 1; j < static_cast<int>(keys.size()); ++j) {
        new_node.SetKeyAt(j - mid - 1, keys[j]);
    }
    for (int j = mid + 1; j < static_cast<int>(children.size()); ++j) {
        new_node.SetChildAt(j - mid - 1, children[j]);
    }
    WUnlatch(new_node_off);
    UnpinPage(new_node_off, true);

    // Write left half back.
    page = PinPage(node_off);
    WLatch(node_off);
    node = InternalPage<K>(page);
    node.SetNumKeys(mid);
    for (int j = 0; j < mid; ++j) {
        node.SetKeyAt(j, keys[j]);
        node.SetChildAt(j, children[j]);
    }
    node.SetChildAt(mid, children[mid]);
    WUnlatch(node_off);
    UnpinPage(node_off, true);

    return true;
}

// ============================================================================
// Delete (with rebalancing)
// ============================================================================

template <typename K, typename Cmp>
Status BPlusTree<K,Cmp>::Delete(const K& key) {
    std::lock_guard<std::mutex> lock(tree_mtx_);

    if (root_offset_ == INVALID_PAGE_ID) return Status::NotFound("key not found");

    // Check existence first so we can return NotFound properly.
    {
        char buf[DATA_SIZE];
        Status s = Search(key, buf);
        if (!s.ok()) return Status::NotFound("key not found");
    }

    bool underful = DeleteRecursive(root_offset_, key);

    if (underful) {
        char* page = PinPage(root_offset_);
        if (!PageIsLeaf(page)) {
            InternalPage<K> root(page);
            if (root.NumKeys() == 0) {
                int64_t old_root = root_offset_;
                root_offset_ = root.ChildAt(0);
                UnpinPage(old_root, false);
                DeallocPage(old_root);
                WriteMetadata();
                return Status::OK();
            }
        } else {
            LeafPage<K> root(page);
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

template <typename K, typename Cmp>
bool BPlusTree<K,Cmp>::DeleteRecursive(int64_t node_off, const K& key) {
    char* page = PinPage(node_off);

    if (PageIsLeaf(page)) {
        UnpinPage(node_off, false);
        return DeleteFromLeaf(node_off, key);
    }

    InternalPage<K> node(page);
    int n = node.NumKeys();
    int i = 0;
    while (i < n && GE(key, node.KeyAt(i))) ++i;
    int64_t child = node.ChildAt(i);
    UnpinPage(node_off, false);

    bool child_underful = DeleteRecursive(child, key);

    if (child_underful) {
        FixChild(node_off, i);

        page = PinPage(node_off);
        InternalPage<K> n2(page);
        int nk = n2.NumKeys();
        UnpinPage(node_off, false);

        if (node_off == root_offset_) return (nk == 0);
        return (nk < InternalPage<K>::kMinKeys);
    }

    return false;
}

template <typename K, typename Cmp>
bool BPlusTree<K,Cmp>::DeleteFromLeaf(int64_t leaf_off, const K& key) {
    char* page = PinPage(leaf_off);
    LeafPage<K> leaf(page);
    int n = leaf.NumKeys();

    int found = -1;
    for (int i = 0; i < n; ++i) {
        if (Equal(leaf.KeyAt(i), key)) { found = i; break; }
    }

    if (found == -1) {
        UnpinPage(leaf_off, false);
        return false;
    }

    // Shift remaining records left.
    for (int j = found; j < n - 1; ++j) {
        K tk{}; char td[DATA_SIZE];
        leaf.GetRecord(j + 1, tk, td);
        leaf.SetRecord(j, tk, td);
    }
    leaf.SetNumKeys(n - 1);
    UnpinPage(leaf_off, true);

    if (leaf_off == root_offset_) return (n - 1 == 0);
    return (n - 1 < LeafPage<K>::kMinKeys);
}

// ============================================================================
// Rebalancing
// ============================================================================

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::FixChild(int64_t parent_off, int child_idx) {
    char* ppage = PinPage(parent_off);
    InternalPage<K> parent(ppage);
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

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::FixLeafChild(int64_t parent_off, int child_idx) {
    char* ppage = PinPage(parent_off);
    InternalPage<K> parent(ppage);
    int parent_keys = parent.NumKeys();
    int64_t child_off = parent.ChildAt(child_idx);

    // Try to borrow from left sibling.
    if (child_idx > 0) {
        int64_t left_off = parent.ChildAt(child_idx - 1);
        UnpinPage(parent_off, false);

        char* lpage = PinPage(left_off);
        LeafPage<K> left(lpage);
        int left_n = left.NumKeys();

        if (left_n > LeafPage<K>::kMinKeys) {
            K tk{}; char td[DATA_SIZE];
            left.GetRecord(left_n - 1, tk, td);
            left.SetNumKeys(left_n - 1);
            UnpinPage(left_off, true);

            char* cpage = PinPage(child_off);
            LeafPage<K> child(cpage);
            int cn = child.NumKeys();
            for (int j = cn - 1; j >= 0; --j) {
                K k2{}; char d2[DATA_SIZE];
                child.GetRecord(j, k2, d2);
                child.SetRecord(j + 1, k2, d2);
            }
            child.SetRecord(0, tk, td);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            ppage = PinPage(parent_off);
            InternalPage<K> p2(ppage);
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
    parent = InternalPage<K>(ppage);
    if (child_idx < parent_keys) {
        int64_t right_off = parent.ChildAt(child_idx + 1);
        UnpinPage(parent_off, false);

        char* rpage = PinPage(right_off);
        LeafPage<K> right(rpage);
        int right_n = right.NumKeys();

        if (right_n > LeafPage<K>::kMinKeys) {
            K tk{}; char td[DATA_SIZE];
            right.GetRecord(0, tk, td);
            for (int j = 0; j < right_n - 1; ++j) {
                K k2{}; char d2[DATA_SIZE];
                right.GetRecord(j + 1, k2, d2);
                right.SetRecord(j, k2, d2);
            }
            right.SetNumKeys(right_n - 1);
            K new_right_first = right.KeyAt(0);
            UnpinPage(right_off, true);

            char* cpage = PinPage(child_off);
            LeafPage<K> child(cpage);
            int cn = child.NumKeys();
            child.SetRecord(cn, tk, td);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            ppage = PinPage(parent_off);
            InternalPage<K> p2(ppage);
            p2.SetKeyAt(child_idx, new_right_first);
            UnpinPage(parent_off, true);
            return;
        }
        UnpinPage(right_off, false);
    } else {
        UnpinPage(parent_off, false);
    }

    // Cannot borrow -- merge.
    ppage = PinPage(parent_off);
    parent = InternalPage<K>(ppage);

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
    UnpinPage(parent_off, false);

    char* lpage = PinPage(left_off);
    LeafPage<K> left(lpage);
    int ln = left.NumKeys();

    char* rpage = PinPage(right_off);
    LeafPage<K> right(rpage);
    int rn = right.NumKeys();

    for (int j = 0; j < rn; ++j) {
        K tk{}; char td[DATA_SIZE];
        right.GetRecord(j, tk, td);
        left.SetRecord(ln + j, tk, td);
    }
    left.SetNumKeys(ln + rn);
    left.SetNextLeaf(right.NextLeaf());

    UnpinPage(left_off, true);
    UnpinPage(right_off, false);
    DeallocPage(right_off);

    ppage = PinPage(parent_off);
    parent = InternalPage<K>(ppage);
    int pn = parent.NumKeys();
    for (int j = merge_key_idx; j < pn - 1; ++j) {
        parent.SetKeyAt(j, parent.KeyAt(j + 1));
        parent.SetChildAt(j + 1, parent.ChildAt(j + 2));
    }
    parent.SetNumKeys(pn - 1);
    UnpinPage(parent_off, true);
}

template <typename K, typename Cmp>
void BPlusTree<K,Cmp>::FixInternalChild(int64_t parent_off, int child_idx) {
    char* ppage = PinPage(parent_off);
    InternalPage<K> parent(ppage);
    int parent_keys = parent.NumKeys();
    int64_t child_off = parent.ChildAt(child_idx);

    // Try to borrow from left sibling.
    if (child_idx > 0) {
        int64_t left_off = parent.ChildAt(child_idx - 1);
        K parent_key = parent.KeyAt(child_idx - 1);
        UnpinPage(parent_off, false);

        char* lpage = PinPage(left_off);
        InternalPage<K> left(lpage);
        int left_n = left.NumKeys();

        if (left_n > InternalPage<K>::kMinKeys) {
            K borrowed_key = left.KeyAt(left_n - 1);
            int64_t borrowed_child = left.ChildAt(left_n);
            left.SetNumKeys(left_n - 1);
            UnpinPage(left_off, true);

            char* cpage = PinPage(child_off);
            InternalPage<K> child(cpage);
            int cn = child.NumKeys();
            for (int j = cn - 1; j >= 0; --j) {
                child.SetKeyAt(j + 1, child.KeyAt(j));
                child.SetChildAt(j + 2, child.ChildAt(j + 1));
            }
            child.SetChildAt(1, child.ChildAt(0));
            child.SetKeyAt(0, parent_key);
            child.SetChildAt(0, borrowed_child);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            ppage = PinPage(parent_off);
            InternalPage<K> p2(ppage);
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
    parent = InternalPage<K>(ppage);
    if (child_idx < parent_keys) {
        int64_t right_off = parent.ChildAt(child_idx + 1);
        K parent_key = parent.KeyAt(child_idx);
        UnpinPage(parent_off, false);

        char* rpage = PinPage(right_off);
        InternalPage<K> right(rpage);
        int right_n = right.NumKeys();

        if (right_n > InternalPage<K>::kMinKeys) {
            K borrowed_key = right.KeyAt(0);
            int64_t borrowed_child = right.ChildAt(0);
            for (int j = 0; j < right_n - 1; ++j) {
                right.SetKeyAt(j, right.KeyAt(j + 1));
                right.SetChildAt(j, right.ChildAt(j + 1));
            }
            right.SetChildAt(right_n - 1, right.ChildAt(right_n));
            right.SetNumKeys(right_n - 1);
            UnpinPage(right_off, true);

            char* cpage = PinPage(child_off);
            InternalPage<K> child(cpage);
            int cn = child.NumKeys();
            child.SetKeyAt(cn, parent_key);
            child.SetChildAt(cn + 1, borrowed_child);
            child.SetNumKeys(cn + 1);
            UnpinPage(child_off, true);

            ppage = PinPage(parent_off);
            InternalPage<K> p2(ppage);
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
    parent = InternalPage<K>(ppage);

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
    K merge_key = parent.KeyAt(merge_key_idx);
    UnpinPage(parent_off, false);

    char* lpage = PinPage(left_off);
    InternalPage<K> left(lpage);
    int ln = left.NumKeys();

    char* rpage = PinPage(right_off);
    InternalPage<K> right(rpage);
    int rn = right.NumKeys();

    // Merge: left + merge_key + right -> left.
    left.SetKeyAt(ln, merge_key);
    left.SetChildAt(ln + 1, right.ChildAt(0));

    for (int j = 0; j < rn; ++j) {
        left.SetKeyAt(ln + 1 + j, right.KeyAt(j));
        left.SetChildAt(ln + 2 + j, right.ChildAt(j + 1));
    }
    left.SetNumKeys(ln + 1 + rn);

    UnpinPage(left_off, true);
    UnpinPage(right_off, false);
    DeallocPage(right_off);

    ppage = PinPage(parent_off);
    parent = InternalPage<K>(ppage);
    int pn = parent.NumKeys();
    for (int j = merge_key_idx; j < pn - 1; ++j) {
        parent.SetKeyAt(j, parent.KeyAt(j + 1));
        parent.SetChildAt(j + 1, parent.ChildAt(j + 2));
    }
    parent.SetNumKeys(pn - 1);
    UnpinPage(parent_off, true);
}

}  // namespace bptree
