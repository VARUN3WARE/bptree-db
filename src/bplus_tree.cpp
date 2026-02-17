/// @file bplus_tree.cpp
/// @brief B+ tree implementation — insert, search, range query, delete.

#include "bptree/bplus_tree.h"
#include "bptree/page.h"

#include <algorithm>
#include <cstring>
#include <vector>

namespace bptree {

// ============================================================================
// Construction / destruction
// ============================================================================

BPlusTree::BPlusTree(const std::string& index_file)
    : disk_(std::make_unique<DiskManager>(index_file))
{
    ReadMetadata();
}

BPlusTree::~BPlusTree() {
    WriteMetadata();
    disk_->Sync();
}

// ============================================================================
// Metadata persistence (page 0)
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

        // Sanity checks
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
// Helpers
// ============================================================================

bool BPlusTree::IsEmpty() const { return root_offset_ == INVALID_PAGE_ID; }

void BPlusTree::Sync() { disk_->Sync(); }

std::string BPlusTree::FilePath() const { return disk_->FilePath(); }

// ============================================================================
// Search helpers
// ============================================================================

int64_t BPlusTree::SearchLeaf(key_t key) const {
    if (root_offset_ == INVALID_PAGE_ID) return INVALID_PAGE_ID;

    int64_t current = root_offset_;

    while (!PageIsLeaf(disk_->PageData(current))) {
        InternalPage node(const_cast<char*>(disk_->PageData(current)));
        int n = node.NumKeys();
        int i = 0;
        while (i < n && key >= node.KeyAt(i)) ++i;

        current = node.ChildAt(i);
        if (current < static_cast<int64_t>(PAGE_SIZE) ||
            current >= static_cast<int64_t>(disk_->FileSize())) {
            return INVALID_PAGE_ID;
        }
    }
    return current;
}

// ============================================================================
// Search (point query)
// ============================================================================

Status BPlusTree::Search(key_t key, char* data_out) const {
    int64_t leaf_off = SearchLeaf(key);
    if (leaf_off == INVALID_PAGE_ID) return Status::NotFound("key not found");

    LeafPage leaf(const_cast<char*>(disk_->PageData(leaf_off)));
    int n = leaf.NumKeys();
    for (int i = 0; i < n; ++i) {
        if (leaf.KeyAt(i) == key) {
            leaf.GetData(i, data_out);
            return Status::OK();
        }
    }
    return Status::NotFound("key not found");
}

Status BPlusTree::Search(key_t key, std::string& value_out) const {
    char buf[DATA_SIZE];
    Status s = Search(key, buf);
    if (s.ok()) {
        // Find actual string length (data may be zero-padded).
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
        LeafPage leaf(const_cast<char*>(disk_->PageData(leaf_off)));
        int n = leaf.NumKeys();

        for (int i = 0; i < n; ++i) {
            int k = leaf.KeyAt(i);
            if (k > upper) return Status::OK();  // done
            if (k >= lower) {
                char buf[DATA_SIZE];
                leaf.GetData(i, buf);
                results.emplace_back(k, std::string(buf, ::strnlen(buf, DATA_SIZE)));
            }
        }

        leaf_off = leaf.NextLeaf();
    }

    return Status::OK();
}

// ============================================================================
// Insert
// ============================================================================

Status BPlusTree::Insert(key_t key, const char* data) {
    // Ensure data is exactly DATA_SIZE bytes (pad with zeros if needed).
    char padded[DATA_SIZE]{};
    std::memcpy(padded, data, std::min(std::strlen(data) + 1, DATA_SIZE));

    // Empty tree — create root leaf.
    if (root_offset_ == INVALID_PAGE_ID) {
        int64_t off = disk_->AllocatePage();
        next_page_offset_ = disk_->NextPageOffset();

        LeafPage::Init(disk_->PageData(off));
        LeafPage leaf(disk_->PageData(off));
        leaf.SetNumKeys(1);
        leaf.SetRecord(0, key, padded);

        root_offset_ = off;
        WriteMetadata();
        return Status::OK();
    }

    // Recursive insert.
    key_t   split_key;
    int64_t new_off;
    bool split = InsertRecursive(root_offset_, key, padded, split_key, new_off);

    if (split) {
        // Create a new root.
        int64_t new_root = disk_->AllocatePage();
        next_page_offset_ = disk_->NextPageOffset();

        InternalPage::Init(disk_->PageData(new_root));
        InternalPage root(disk_->PageData(new_root));
        root.SetNumKeys(1);
        root.SetKeyAt(0, split_key);
        root.SetChildAt(0, root_offset_);
        root.SetChildAt(1, new_off);

        root_offset_ = new_root;
        WriteMetadata();
    }

    disk_->SyncAsync();
    return Status::OK();
}

// ----------------------------------------------------------------------------

bool BPlusTree::InsertRecursive(int64_t node_off, key_t key, const char* data,
                                key_t& split_key, int64_t& new_off) {
    if (PageIsLeaf(disk_->PageData(node_off))) {
        return InsertIntoLeaf(node_off, key, data, split_key, new_off);
    }

    InternalPage node(disk_->PageData(node_off));
    int n = node.NumKeys();
    int i = 0;
    while (i < n && key >= node.KeyAt(i)) ++i;

    key_t   child_split;
    int64_t child_new;
    bool child_did_split = InsertRecursive(node.ChildAt(i), key, data,
                                           child_split, child_new);
    if (!child_did_split) return false;

    return InsertIntoInternal(node_off, child_split, child_new, split_key, new_off);
}

// ----------------------------------------------------------------------------

bool BPlusTree::InsertIntoLeaf(int64_t leaf_off, key_t key, const char* data,
                               key_t& split_key, int64_t& new_leaf_off) {
    LeafPage leaf(disk_->PageData(leaf_off));
    int n = leaf.NumKeys();

    // Check for existing key → update.
    for (int i = 0; i < n; ++i) {
        if (leaf.KeyAt(i) == key) {
            leaf.SetData(i, data);
            return false;  // no split
        }
    }

    // Room available — insert in sorted position.
    if (n < LEAF_MAX_KEYS) {
        int i = n - 1;
        while (i >= 0 && leaf.KeyAt(i) > key) {
            int    tk; char td[DATA_SIZE];
            leaf.GetRecord(i, tk, td);
            leaf.SetRecord(i + 1, tk, td);
            --i;
        }
        leaf.SetRecord(i + 1, key, data);
        leaf.SetNumKeys(n + 1);
        return false;
    }

    // --- Node is full — split ---

    // Collect all records + the new one into a sorted vector.
    struct Rec { int k; char d[DATA_SIZE]; };
    std::vector<Rec> recs(n);
    for (int i = 0; i < n; ++i) leaf.GetRecord(i, recs[i].k, recs[i].d);

    // Insert new record in sorted position.
    Rec nr; nr.k = key; std::memcpy(nr.d, data, DATA_SIZE);
    auto it = std::lower_bound(recs.begin(), recs.end(), nr,
                               [](const Rec& a, const Rec& b){ return a.k < b.k; });
    recs.insert(it, nr);

    size_t mid = (recs.size() + 1) / 2;

    // Right half goes to new leaf.
    new_leaf_off = disk_->AllocatePage();
    next_page_offset_ = disk_->NextPageOffset();

    // IMPORTANT: AllocatePage() may have remapped the file, so all previous
    // page pointers are potentially stale.  Re-obtain them.
    leaf = LeafPage(disk_->PageData(leaf_off));

    // Re-write left half to the (possibly new) mapping.
    leaf.SetNumKeys(static_cast<int>(mid));
    for (size_t i = 0; i < mid; ++i) leaf.SetRecord(i, recs[i].k, recs[i].d);

    LeafPage::Init(disk_->PageData(new_leaf_off));
    LeafPage new_leaf(disk_->PageData(new_leaf_off));
    new_leaf.SetNumKeys(static_cast<int>(recs.size() - mid));
    for (size_t i = mid; i < recs.size(); ++i) {
        new_leaf.SetRecord(static_cast<int>(i - mid), recs[i].k, recs[i].d);
    }

    // Maintain the leaf linked-list.
    new_leaf.SetNextLeaf(leaf.NextLeaf());
    leaf.SetNextLeaf(new_leaf_off);

    split_key = new_leaf.KeyAt(0);
    return true;
}

// ----------------------------------------------------------------------------

bool BPlusTree::InsertIntoInternal(int64_t node_off, key_t key, int64_t child_off,
                                   key_t& split_key, int64_t& new_node_off) {
    InternalPage node(disk_->PageData(node_off));
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
        return false;
    }

    // --- Full — split ---

    std::vector<int>     keys(n);
    std::vector<int64_t> children(n + 1);
    for (int i = 0; i < n; ++i) keys[i] = node.KeyAt(i);
    for (int i = 0; i <= n; ++i) children[i] = node.ChildAt(i);

    // Insert the new key/child.
    int pos = 0;
    while (pos < static_cast<int>(keys.size()) && keys[pos] < key) ++pos;
    keys.insert(keys.begin() + pos, key);
    children.insert(children.begin() + pos + 1, child_off);

    int mid = static_cast<int>(keys.size()) / 2;
    split_key = keys[mid];

    // Right half in new node — allocate first, then re-obtain pointers
    // (AllocatePage may remap the file).
    new_node_off = disk_->AllocatePage();
    next_page_offset_ = disk_->NextPageOffset();

    // Re-obtain node pointer after potential remap.
    node = InternalPage(disk_->PageData(node_off));

    // Write left half.
    node.SetNumKeys(mid);
    for (int j = 0; j < mid; ++j) {
        node.SetKeyAt(j, keys[j]);
        node.SetChildAt(j, children[j]);
    }
    node.SetChildAt(mid, children[mid]);

    InternalPage::Init(disk_->PageData(new_node_off));
    InternalPage new_node(disk_->PageData(new_node_off));

    int right_count = static_cast<int>(keys.size()) - mid - 1;
    new_node.SetNumKeys(right_count);
    for (int j = mid + 1; j < static_cast<int>(keys.size()); ++j) {
        new_node.SetKeyAt(j - mid - 1, keys[j]);
    }
    for (int j = mid + 1; j < static_cast<int>(children.size()); ++j) {
        new_node.SetChildAt(j - mid - 1, children[j]);
    }

    return true;
}

// ============================================================================
// Delete
// ============================================================================

Status BPlusTree::Delete(key_t key) {
    int64_t leaf_off = SearchLeaf(key);
    if (leaf_off == INVALID_PAGE_ID) return Status::NotFound("key not found");

    LeafPage leaf(disk_->PageData(leaf_off));
    int n = leaf.NumKeys();

    for (int i = 0; i < n; ++i) {
        if (leaf.KeyAt(i) == key) {
            // Shift remaining records left.
            for (int j = i; j < n - 1; ++j) {
                int    tk; char td[DATA_SIZE];
                leaf.GetRecord(j + 1, tk, td);
                leaf.SetRecord(j, tk, td);
            }
            leaf.SetNumKeys(n - 1);
            disk_->SyncAsync();
            return Status::OK();
        }
    }

    return Status::NotFound("key not found");
}

}  // namespace bptree
