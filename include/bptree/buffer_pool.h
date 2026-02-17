#pragma once

/// @file buffer_pool.h
/// @brief LRU buffer pool that caches fixed-size pages between the B+ tree
///        and the DiskManager.
///
/// Design:
///   - Fixed number of in-memory page frames (configurable at construction).
///   - Each frame has a pin count; only unpinned frames are eviction candidates.
///   - A dirty flag triggers write-back on eviction.
///   - LRU replacement policy via a doubly-linked list.
///
/// Typical usage:
/// @code
///   BufferPool pool(disk_manager, /*pool_size=*/256);
///   auto* page = pool.FetchPage(offset);   // pins page, returns writable ptr
///   // ... read / write through page->Data() ...
///   pool.UnpinPage(offset, /*dirty=*/true);
///   pool.FlushAllPages();
/// @endcode

#include "config.h"
#include "disk_manager.h"

#include <cstdint>
#include <list>
#include <unordered_map>
#include <vector>

namespace bptree { class WriteAheadLog; }  // forward declaration

namespace bptree {

/// Metadata kept per in-memory page frame.
struct PageFrame {
    int64_t page_id   = INVALID_PAGE_ID;  ///< Byte offset in the file.
    int     pin_count = 0;                ///< Number of active users.
    bool    dirty     = false;            ///< True if modified since last flush.
    char    data[PAGE_SIZE]{};            ///< In-memory copy of the page.
};

/// An LRU buffer pool that sits between the B+ tree and the DiskManager.
///
/// The pool owns a fixed number of page frames.  Pages are read from disk on
/// first access and cached.  When the pool is full and a new page is needed,
/// the least-recently-used unpinned frame is evicted (flushed if dirty).
///
/// Pin semantics:
///   - `FetchPage` increments pin_count and returns a writable pointer.
///   - `UnpinPage` decrements pin_count.  Only frames with pin_count == 0
///     are eligible for eviction.
///   - Callers MUST unpin every page they fetch.
class BufferPool {
public:
    /// Create a buffer pool with @p pool_size page frames backed by @p disk.
    /// @param disk      The disk manager to read/write pages from.
    /// @param pool_size Number of page frames (default 1024 = 4 MB).
    explicit BufferPool(DiskManager& disk, size_t pool_size = 1024);

    ~BufferPool();

    // Non-copyable, non-movable.
    BufferPool(const BufferPool&)            = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    /// Fetch the page at @p page_id (byte offset) into the pool.
    /// Increments pin_count.  Returns a writable pointer into the frame's
    /// data buffer.
    ///
    /// @return nullptr if the page cannot be fetched (all frames pinned).
    char* FetchPage(int64_t page_id);

    /// Decrement pin_count for @p page_id.  Mark dirty if @p dirty is true.
    /// @return false if the page is not in the pool.
    bool UnpinPage(int64_t page_id, bool dirty);

    /// Write a dirty page back to disk without evicting it.
    /// @return false if the page is not in the pool.
    bool FlushPage(int64_t page_id);

    /// Flush all dirty pages to disk.
    void FlushAllPages();

    /// Allocate a new page via DiskManager and bring it into the pool (pinned).
    /// @param[out] page_id  Receives the byte offset of the new page.
    /// @return Writable pointer to the new page's data, or nullptr on failure.
    char* NewPage(int64_t& page_id);

    /// Remove a page from the pool (e.g. after freeing it in a free-list).
    /// The page must be unpinned.
    /// @return false if the page is pinned or not in the pool.
    bool DeletePage(int64_t page_id);

    // -- WAL integration ----------------------------------------------------

    /// Attach a WAL to the buffer pool.  When set, dirty pages are logged
    /// to the WAL before being written to disk (WAL protocol).
    void SetWAL(WriteAheadLog* wal) { wal_ = wal; }

    // -- Statistics ----------------------------------------------------------

    [[nodiscard]] size_t PoolSize()    const { return pool_size_; }
    [[nodiscard]] size_t PagesInUse()  const { return page_table_.size(); }
    [[nodiscard]] size_t HitCount()    const { return hits_; }
    [[nodiscard]] size_t MissCount()   const { return misses_; }
    [[nodiscard]] double HitRate()     const {
        size_t total = hits_ + misses_;
        return total == 0 ? 0.0 : static_cast<double>(hits_) / total;
    }

private:
    /// Find a victim frame to evict (LRU among unpinned).
    /// Returns the index into frames_, or -1 if all frames are pinned.
    int FindVictim();

    /// Evict a frame: flush if dirty, remove from page table and LRU list.
    void EvictFrame(int frame_idx);

    /// Touch a frame: move it to the back of the LRU list (most recent).
    void TouchFrame(int frame_idx);

    DiskManager& disk_;
    size_t       pool_size_;

    std::vector<PageFrame> frames_;

    /// Maps page_id (byte offset) -> frame index.
    std::unordered_map<int64_t, int> page_table_;

    /// LRU list: front = least recently used, back = most recently used.
    /// Stores frame indices.  Only unpinned frames appear in this list.
    std::list<int> lru_list_;

    /// Reverse map: frame index -> iterator into lru_list_ (for O(1) removal).
    std::unordered_map<int, std::list<int>::iterator> lru_map_;

    /// Free frame indices (frames not currently holding any page).
    std::vector<int> free_list_;

    /// Optional WAL for crash recovery (not owned).
    WriteAheadLog* wal_ = nullptr;

    // Stats
    size_t hits_   = 0;
    size_t misses_ = 0;
};

}  // namespace bptree
