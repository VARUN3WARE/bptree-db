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
#include "latch.h"
#include "comparator.h"

#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <utility>

namespace bptree {

// Forward declarations
template <typename K, typename Cmp> class BPlusTree;
class TreeVisualizer;

/// A persistent, disk-backed B+ tree index.
///
/// Template parameters:
///   K    -- Key type stored in the index.  Must match KeyTraits<K> and Cmp.
///   Cmp  -- Comparator type with bool operator()(K, K) semantics (less-than).
///
/// Supports fixed-size (100-byte) data payloads.
/// Data is stored on disk via memory-mapped I/O and survives restarts.
/// A buffer pool (LRU) sits between the tree and disk to cache hot pages.
///
/// Delete operations properly rebalance the tree by redistributing or
/// merging underful nodes.
///
/// @par Thread safety
/// Thread-safe for concurrent readers and writers via latch crabbing.
/// Each page frame carries a reader-writer latch; the tree acquires and
/// releases latches top-down (crab-walk) so readers never block each other
/// and writers only hold the minimum set of pages needed. :)
///
/// @par Example
/// @code
///   // Integer keys (default):
///   bptree::BPlusTree<int> tree("my_index.idx");
///   tree.Insert(42, "hello world");
///
///   // String keys:
///   bptree::BPlusTree<std::string> stree("str_index.idx");
///   stree.Insert("foo", "bar data");
/// @endcode
template <typename K = int, typename Cmp = DefaultComparator<K>>
class BPlusTree {
public:
    using key_type = K;
    using cmp_type = Cmp;

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
    Status Insert(const K& key, const char* data);

    /// Point lookup (raw buffer).
    Status Search(const K& key, char* data_out) const;

    /// Point lookup (std::string).
    Status Search(const K& key, std::string& value_out) const;

    /// Delete a key.  Rebalances underful nodes via redistribute / merge.
    Status Delete(const K& key);

    /// Range query -- returns all records with keys in [lower, upper].
    Status RangeQuery(const K& lower, const K& upper,
                      std::vector<std::pair<K, std::string>>& results) const;

    // -- Utilities -----------------------------------------------------------

    [[nodiscard]] bool IsEmpty() const;
    void Sync();
    [[nodiscard]] std::string FilePath() const;

    /// Force a WAL checkpoint: flush all dirty pages, then truncate the log.
    void Checkpoint();

    /// Buffer pool statistics.
    [[nodiscard]] size_t BufferPoolHits()    const;
    [[nodiscard]] size_t BufferPoolMisses()  const;
    [[nodiscard]] double BufferPoolHitRate() const;

    /// WAL statistics.
    [[nodiscard]] size_t WALBytesWritten()   const;
    [[nodiscard]] size_t WALRecordsWritten() const;
    [[nodiscard]] bool   WALEnabled()        const;

    // Allow visualizer to inspect tree internals (int specialization only)
    friend class TreeVisualizer;

private:
    // -- Page access helpers (through buffer pool) ---------------------------
    char* PinPage(int64_t page_id) const;
    void  UnpinPage(int64_t page_id, bool dirty) const;
    char* AllocPage(int64_t& page_id);
    void  DeallocPage(int64_t page_id);

    // -- Latch helpers (delegated to BufferPool) -----------------------------
    void RLatch(int64_t page_id)   const;
    void RUnlatch(int64_t page_id) const;
    void WLatch(int64_t page_id)   const;
    void WUnlatch(int64_t page_id) const;

    // -- Tree navigation -----------------------------------------------------
    int64_t SearchLeaf(const K& key) const;

    // -- Insert helpers ------------------------------------------------------
    bool InsertRecursive(int64_t node_off, const K& key, const char* data,
                         K& split_key, int64_t& new_off);
    bool InsertIntoLeaf(int64_t leaf_off, const K& key, const char* data,
                        K& split_key, int64_t& new_leaf_off);
    bool InsertIntoInternal(int64_t node_off, const K& key, int64_t child_off,
                            K& split_key, int64_t& new_node_off);

    // -- Delete helpers (with rebalancing) ------------------------------------
    bool DeleteRecursive(int64_t node_off, const K& key);
    bool DeleteFromLeaf(int64_t leaf_off, const K& key);
    void FixChild(int64_t parent_off, int child_idx);
    void FixLeafChild(int64_t parent_off, int child_idx);
    void FixInternalChild(int64_t parent_off, int child_idx);

    // -- Metadata ------------------------------------------------------------
    void WriteMetadata();
    void ReadMetadata();

    // -- Comparator helpers (so code reads like key1 < key2 not cmp_(k1,k2)) -
    bool Less(const K& a, const K& b)  const { return cmp_(a, b); }
    bool Equal(const K& a, const K& b) const { return cmp_.Equal(a, b); }
    bool GE(const K& a, const K& b)    const { return !cmp_(a, b); }  // a >= b

    // -- State ---------------------------------------------------------------
    std::unique_ptr<DiskManager>   disk_;
    std::unique_ptr<WriteAheadLog> wal_;    ///< Destroyed AFTER pool_.
    std::unique_ptr<BufferPool>    pool_;   ///< Destroyed first (may flush via WAL).
    int64_t root_offset_      = INVALID_PAGE_ID;
    int64_t next_page_offset_ = PAGE_SIZE;
    Cmp     cmp_{};                         ///< Key comparator instance.

    /// Tree-level mutex serialises all write operations (Insert / Delete).
    /// Reads (Search, RangeQuery) use only page-level latches and can run
    /// truly concurrently with each other.
    mutable std::mutex tree_mtx_;
};

// ============================================================================
// Convenience type aliases
// ============================================================================

/// The classic int-key tree (default, backward-compatible).
using IntTree    = BPlusTree<int>;

/// 64-bit integer key tree.
using BigIntTree = BPlusTree<int64_t>;

/// String key tree (fixed 64-byte on-disk key, lexicographic order).
using StrTree    = BPlusTree<std::string>;

}  // namespace bptree

// Include the template implementation.
// We use the "include the .tpp file" pattern so the .cpp stays clean.
#include "bplus_tree.tpp"
