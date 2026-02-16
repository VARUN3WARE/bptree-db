#pragma once

/// @file bplus_tree.h
/// @brief Disk-based B+ tree index with insert, point query, range query,
///        and delete.  Uses DiskManager for page-level I/O.

#include "config.h"
#include "status.h"
#include "disk_manager.h"

#include <memory>
#include <string>
#include <vector>
#include <utility>

namespace bptree {

/// A persistent, disk-backed B+ tree index.
///
/// Supports integer keys and fixed-size (100-byte) data payloads.
/// Data is stored on disk via memory-mapped I/O and survives restarts.
///
/// @par Thread safety
/// Not thread-safe.  External locking is required for concurrent access.
///
/// @par Example
/// @code
///   bptree::BPlusTree tree("my_index.idx");
///   tree.Insert(42, "hello world");
///
///   std::string value;
///   if (tree.Search(42, value).ok()) {
///       std::cout << value << std::endl;
///   }
/// @endcode
class BPlusTree {
public:
    /// Open (or create) a B+ tree backed by the given file.
    explicit BPlusTree(const std::string& index_file = DEFAULT_INDEX_FILE);
    ~BPlusTree();

    // Non-copyable
    BPlusTree(const BPlusTree&)            = delete;
    BPlusTree& operator=(const BPlusTree&) = delete;

    // -- Core operations -----------------------------------------------------

    /// Insert a key-value pair.  If the key already exists the value is
    /// **updated** in place (upsert semantics).
    /// @param key   Integer key.
    /// @param data  Null-terminated string (max 99 chars) or raw 100-byte buf.
    /// @return Status::OK() on success.
    Status Insert(key_t key, const char* data);

    /// Point lookup.
    /// @param[in]  key       Key to search for.
    /// @param[out] data_out  Receives the 100-byte payload on success.
    /// @return Status::OK() if found, Status::NotFound() otherwise.
    Status Search(key_t key, char* data_out) const;

    /// Search returning a std::string for convenience.
    Status Search(key_t key, std::string& value_out) const;

    /// Delete a key.
    /// @return Status::OK() on success, Status::NotFound() if absent.
    /// @note  Current implementation does **not** rebalance after deletion.
    Status Delete(key_t key);

    /// Range query — returns all records with keys in [lower, upper].
    /// Results are sorted by key.
    Status RangeQuery(key_t lower, key_t upper,
                      std::vector<std::pair<key_t, std::string>>& results) const;

    // -- Utilities -----------------------------------------------------------

    /// True if the tree has no records.
    [[nodiscard]] bool IsEmpty() const;

    /// Flush all data to disk.
    void Sync();

    /// Return the underlying file path.
    [[nodiscard]] std::string FilePath() const;

private:
    // -- Internal helpers ----------------------------------------------------
    int64_t SearchLeaf(key_t key) const;

    bool InsertIntoLeaf(int64_t leaf_off, key_t key, const char* data,
                        key_t& split_key, int64_t& new_leaf_off);

    bool InsertIntoInternal(int64_t node_off, key_t key, int64_t child_off,
                            key_t& split_key, int64_t& new_node_off);

    bool InsertRecursive(int64_t node_off, key_t key, const char* data,
                         key_t& split_key, int64_t& new_off);

    void WriteMetadata();
    void ReadMetadata();

    // -- State ---------------------------------------------------------------
    std::unique_ptr<DiskManager> disk_;
    int64_t root_offset_      = INVALID_PAGE_ID;
    int64_t next_page_offset_ = PAGE_SIZE;
};

}  // namespace bptree
