#pragma once

/// @file bplus_tree.h
/// @brief Disk-based B+ tree index with insert, point query, range query,
///        and delete with rebalancing.  Uses BufferPool for page-level caching
///        over DiskManager.

#include "config.h"
#include "status.h"
#include "disk_manager.h"
#include "buffer_pool.h"
#include "wal.h"

#include <memory>
#include <string>
#include <vector>
#include <utility>

namespace bptree {

/// A persistent, disk-backed B+ tree index.
///
/// Supports integer keys and fixed-size (100-byte) data payloads.
/// Data is stored on disk via memory-mapped I/O and survives restarts.
/// A buffer pool (LRU) sits between the tree and the disk to cache hot pages.
///
/// Delete operations properly rebalance the tree by redistributing or
/// merging underful nodes.
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
    /// @param index_file  Path to the index file.
    /// @param pool_size   Number of buffer pool frames (default 1024 = 4 MB).
    /// @param enable_wal  Enable write-ahead logging for crash recovery.
    explicit BPlusTree(const std::string& index_file = DEFAULT_INDEX_FILE,
                       size_t pool_size = DEFAULT_POOL_SIZE,
                       bool enable_wal = true);
    ~BPlusTree();

    // Non-copyable
    BPlusTree(const BPlusTree&)            = delete;
    BPlusTree& operator=(const BPlusTree&) = delete;

    // -- Core operations -----------------------------------------------------

    /// Insert a key-value pair (upsert semantics).
    Status Insert(key_t key, const char* data);

    /// Point lookup (raw buffer).
    Status Search(key_t key, char* data_out) const;

    /// Point lookup (std::string).
    Status Search(key_t key, std::string& value_out) const;

    /// Delete a key.  Rebalances underful nodes via redistribute / merge.
    Status Delete(key_t key);

    /// Range query -- returns all records with keys in [lower, upper].
    Status RangeQuery(key_t lower, key_t upper,
                      std::vector<std::pair<key_t, std::string>>& results) const;

    // -- Utilities -----------------------------------------------------------

    [[nodiscard]] bool IsEmpty() const;
    void Sync();
    [[nodiscard]] std::string FilePath() const;

    /// Force a WAL checkpoint: flush all dirty pages, then truncate the log.
    void Checkpoint();

    /// Buffer pool statistics.
    [[nodiscard]] size_t BufferPoolHits()   const;
    [[nodiscard]] size_t BufferPoolMisses() const;
    [[nodiscard]] double BufferPoolHitRate() const;

    /// WAL statistics.
    [[nodiscard]] size_t WALBytesWritten()   const;
    [[nodiscard]] size_t WALRecordsWritten() const;
    [[nodiscard]] bool   WALEnabled()        const;

private:
    // -- Page access helpers (through buffer pool) ---------------------------
    char* PinPage(int64_t page_id) const;
    void  UnpinPage(int64_t page_id, bool dirty) const;
    char* AllocPage(int64_t& page_id);
    void  DeallocPage(int64_t page_id);

    // -- Tree navigation -----------------------------------------------------
    int64_t SearchLeaf(key_t key) const;

    // -- Insert helpers ------------------------------------------------------
    bool InsertRecursive(int64_t node_off, key_t key, const char* data,
                         key_t& split_key, int64_t& new_off);
    bool InsertIntoLeaf(int64_t leaf_off, key_t key, const char* data,
                        key_t& split_key, int64_t& new_leaf_off);
    bool InsertIntoInternal(int64_t node_off, key_t key, int64_t child_off,
                            key_t& split_key, int64_t& new_node_off);

    // -- Delete helpers (with rebalancing) ------------------------------------
    // Returns true if the child became underful and the parent should fix it.
    bool DeleteRecursive(int64_t node_off, key_t key);
    bool DeleteFromLeaf(int64_t leaf_off, key_t key);
    void FixChild(int64_t parent_off, int child_idx);
    void FixLeafChild(int64_t parent_off, int child_idx);
    void FixInternalChild(int64_t parent_off, int child_idx);

    // -- Metadata ------------------------------------------------------------
    void WriteMetadata();
    void ReadMetadata();

    // -- State ---------------------------------------------------------------
    std::unique_ptr<DiskManager>   disk_;
    std::unique_ptr<BufferPool>    pool_;
    std::unique_ptr<WriteAheadLog> wal_;
    int64_t root_offset_      = INVALID_PAGE_ID;
    int64_t next_page_offset_ = PAGE_SIZE;
};

}  // namespace bptree
