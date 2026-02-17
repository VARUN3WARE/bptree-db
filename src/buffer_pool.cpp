/// @file buffer_pool.cpp
/// @brief LRU buffer pool implementation.

#include "bptree/buffer_pool.h"

#include <cassert>
#include <cstring>

namespace bptree {

// ============================================================================
// Construction / destruction
// ============================================================================

BufferPool::BufferPool(DiskManager& disk, size_t pool_size)
    : disk_(disk), pool_size_(pool_size), frames_(pool_size)
{
    // All frames start on the free list.
    free_list_.reserve(pool_size);
    for (int i = static_cast<int>(pool_size) - 1; i >= 0; --i) {
        free_list_.push_back(i);
    }
}

BufferPool::~BufferPool() {
    FlushAllPages();
}

// ============================================================================
// FetchPage
// ============================================================================

char* BufferPool::FetchPage(int64_t page_id) {
    assert(page_id >= 0);

    // Already in pool?
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        ++hits_;
        int idx = it->second;
        PageFrame& f = frames_[idx];
        ++f.pin_count;
        // If it was in the LRU (unpinned), remove it (pinned pages are not
        // eviction candidates).
        auto lru_it = lru_map_.find(idx);
        if (lru_it != lru_map_.end()) {
            lru_list_.erase(lru_it->second);
            lru_map_.erase(lru_it);
        }
        return f.data;
    }

    // Cache miss -- need a free frame.
    ++misses_;
    int frame_idx = -1;

    if (!free_list_.empty()) {
        frame_idx = free_list_.back();
        free_list_.pop_back();
    } else {
        frame_idx = FindVictim();
        if (frame_idx == -1) return nullptr;  // all frames pinned
        EvictFrame(frame_idx);
    }

    // Read page from disk into frame.
    PageFrame& f = frames_[frame_idx];
    f.page_id   = page_id;
    f.pin_count = 1;
    f.dirty     = false;

    const char* disk_page = disk_.PageData(page_id);
    std::memcpy(f.data, disk_page, PAGE_SIZE);

    page_table_[page_id] = frame_idx;
    return f.data;
}

// ============================================================================
// UnpinPage
// ============================================================================

bool BufferPool::UnpinPage(int64_t page_id, bool dirty) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;

    int idx = it->second;
    PageFrame& f = frames_[idx];

    if (f.pin_count <= 0) return false;

    if (dirty) f.dirty = true;
    --f.pin_count;

    // When pin_count reaches 0, add to LRU list (now eligible for eviction).
    if (f.pin_count == 0) {
        lru_list_.push_back(idx);
        lru_map_[idx] = std::prev(lru_list_.end());
    }
    return true;
}

// ============================================================================
// FlushPage / FlushAllPages
// ============================================================================

bool BufferPool::FlushPage(int64_t page_id) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;

    int idx = it->second;
    PageFrame& f = frames_[idx];

    if (f.dirty) {
        char* disk_page = disk_.PageData(page_id);
        std::memcpy(disk_page, f.data, PAGE_SIZE);
        f.dirty = false;
    }
    return true;
}

void BufferPool::FlushAllPages() {
    for (auto& [pid, idx] : page_table_) {
        PageFrame& f = frames_[idx];
        if (f.dirty) {
            char* disk_page = disk_.PageData(pid);
            std::memcpy(disk_page, f.data, PAGE_SIZE);
            f.dirty = false;
        }
    }
    disk_.Sync();
}

// ============================================================================
// NewPage
// ============================================================================

char* BufferPool::NewPage(int64_t& page_id) {
    // Allocate on disk first.
    page_id = disk_.AllocatePage();

    // Find a frame for it.
    int frame_idx = -1;
    if (!free_list_.empty()) {
        frame_idx = free_list_.back();
        free_list_.pop_back();
    } else {
        frame_idx = FindVictim();
        if (frame_idx == -1) {
            // Cannot evict -- caller should flush.
            return nullptr;
        }
        EvictFrame(frame_idx);
    }

    PageFrame& f = frames_[frame_idx];
    f.page_id   = page_id;
    f.pin_count = 1;
    f.dirty     = true;   // new pages are dirty by definition
    std::memset(f.data, 0, PAGE_SIZE);

    page_table_[page_id] = frame_idx;
    return f.data;
}

// ============================================================================
// DeletePage
// ============================================================================

bool BufferPool::DeletePage(int64_t page_id) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return true;  // not in pool, nothing to do

    int idx = it->second;
    PageFrame& f = frames_[idx];
    if (f.pin_count > 0) return false;  // still pinned

    // Remove from LRU.
    auto lru_it = lru_map_.find(idx);
    if (lru_it != lru_map_.end()) {
        lru_list_.erase(lru_it->second);
        lru_map_.erase(lru_it);
    }

    // Do not flush -- the page is being freed.
    f.page_id   = INVALID_PAGE_ID;
    f.pin_count = 0;
    f.dirty     = false;

    page_table_.erase(it);
    free_list_.push_back(idx);
    return true;
}

// ============================================================================
// LRU internals
// ============================================================================

int BufferPool::FindVictim() {
    // Front of lru_list_ is the least recently used unpinned frame.
    if (lru_list_.empty()) return -1;
    return lru_list_.front();
}

void BufferPool::EvictFrame(int frame_idx) {
    PageFrame& f = frames_[frame_idx];

    // Flush to disk if dirty.
    if (f.dirty && f.page_id != INVALID_PAGE_ID) {
        char* disk_page = disk_.PageData(f.page_id);
        std::memcpy(disk_page, f.data, PAGE_SIZE);
        f.dirty = false;
    }

    // Remove from page table.
    page_table_.erase(f.page_id);

    // Remove from LRU list.
    auto lru_it = lru_map_.find(frame_idx);
    if (lru_it != lru_map_.end()) {
        lru_list_.erase(lru_it->second);
        lru_map_.erase(lru_it);
    }

    f.page_id   = INVALID_PAGE_ID;
    f.pin_count = 0;
}

void BufferPool::TouchFrame(int frame_idx) {
    // Remove from current position and re-add at the back.
    auto lru_it = lru_map_.find(frame_idx);
    if (lru_it != lru_map_.end()) {
        lru_list_.erase(lru_it->second);
        lru_map_.erase(lru_it);
    }
    lru_list_.push_back(frame_idx);
    lru_map_[frame_idx] = std::prev(lru_list_.end());
}

}  // namespace bptree
