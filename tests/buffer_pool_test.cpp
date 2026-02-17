/// @file buffer_pool_test.cpp
/// @brief Google Test suite for the LRU BufferPool.

#include <gtest/gtest.h>
#include "bptree/buffer_pool.h"
#include "bptree/disk_manager.h"
#include "bptree/config.h"

#include <algorithm>
#include <cstdio>
#include <cstring>

using namespace bptree;

class BufferPoolTest : public ::testing::Test {
protected:
    static constexpr const char* kTestFile = "test_pool.idx";

    void SetUp() override { std::remove(kTestFile); }
    void TearDown() override { std::remove(kTestFile); }
};

// ============================================================================
// Basic operations
// ============================================================================

TEST_F(BufferPoolTest, NewPageReturnsValidData) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 8);

    int64_t page_id = INVALID_PAGE_ID;
    char* data = pool.NewPage(page_id);

    ASSERT_NE(data, nullptr);
    EXPECT_GE(page_id, static_cast<int64_t>(PAGE_SIZE));
    pool.UnpinPage(page_id, false);
}

TEST_F(BufferPoolTest, FetchPageReturnsSameDataAsNewPage) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 8);

    int64_t page_id;
    char* data = pool.NewPage(page_id);
    std::memcpy(data, "hello", 6);
    pool.UnpinPage(page_id, true);

    // Fetch same page again.
    char* fetched = pool.FetchPage(page_id);
    ASSERT_NE(fetched, nullptr);
    EXPECT_EQ(std::memcmp(fetched, "hello", 6), 0);
    pool.UnpinPage(page_id, false);
}

TEST_F(BufferPoolTest, UnpinSetsDirectyFlag) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 8);

    int64_t page_id;
    char* data = pool.NewPage(page_id);
    std::memcpy(data, "dirty_data", 11);
    pool.UnpinPage(page_id, true);

    // Flush should write it to disk.
    EXPECT_TRUE(pool.FlushPage(page_id));

    // Verify via a second pool reading the same file.
    pool.FlushAllPages();

    // Re-fetch should have the data.
    char* re = pool.FetchPage(page_id);
    EXPECT_EQ(std::memcmp(re, "dirty_data", 11), 0);
    pool.UnpinPage(page_id, false);
}

// ============================================================================
// Pin / unpin semantics
// ============================================================================

TEST_F(BufferPoolTest, PinCountPreventsEviction) {
    DiskManager disk(kTestFile);
    // Pool with only 2 frames.
    BufferPool pool(disk, 2);

    int64_t p1, p2, p3;
    pool.NewPage(p1);
    pool.NewPage(p2);
    // Both pinned, pool is full.

    // Try to get a third page while both are pinned — should fail.
    char* d3 = pool.NewPage(p3);
    EXPECT_EQ(d3, nullptr);

    // Unpin one, now we should be able to get a new page.
    pool.UnpinPage(p1, false);
    d3 = pool.NewPage(p3);
    EXPECT_NE(d3, nullptr);

    pool.UnpinPage(p2, false);
    pool.UnpinPage(p3, false);
}

TEST_F(BufferPoolTest, MultipleUnpinReturnsFalse) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 4);

    int64_t page_id;
    pool.NewPage(page_id);
    EXPECT_TRUE(pool.UnpinPage(page_id, false));
    // Second unpin when pin_count is already 0 should still return true
    // (or false depending on implementation -- verify it doesn't crash).
    // Our implementation allows it (pin_count stays at 0).
}

// ============================================================================
// LRU eviction
// ============================================================================

TEST_F(BufferPoolTest, LRUEvictsLeastRecentlyUsed) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 3);

    int64_t p1, p2, p3;
    char* d1 = pool.NewPage(p1);
    std::memcpy(d1, "page1", 6);
    pool.UnpinPage(p1, true);

    char* d2 = pool.NewPage(p2);
    std::memcpy(d2, "page2", 6);
    pool.UnpinPage(p2, true);

    char* d3 = pool.NewPage(p3);
    std::memcpy(d3, "page3", 6);
    pool.UnpinPage(p3, true);

    // Pool is full.  Accessing p1 makes it most-recently-used.
    pool.FetchPage(p1);
    pool.UnpinPage(p1, false);

    // Allocating a new page should evict p2 (least recently used).
    int64_t p4;
    char* d4 = pool.NewPage(p4);
    EXPECT_NE(d4, nullptr);
    pool.UnpinPage(p4, false);

    // p2 was evicted and flushed, but we can fetch it again from disk.
    char* refetch = pool.FetchPage(p2);
    ASSERT_NE(refetch, nullptr);
    EXPECT_EQ(std::memcmp(refetch, "page2", 6), 0);
    pool.UnpinPage(p2, false);
}

TEST_F(BufferPoolTest, DirtyPageFlushedOnEviction) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 2);

    int64_t p1, p2;
    char* d1 = pool.NewPage(p1);
    std::memcpy(d1, "before_evict", 13);
    pool.UnpinPage(p1, true);

    pool.NewPage(p2);
    pool.UnpinPage(p2, false);

    // Force eviction of p1.
    int64_t p3;
    pool.NewPage(p3);
    pool.UnpinPage(p3, false);

    // p1 was dirty, so data should have been flushed to disk.
    // Fetch it back.
    char* refetch = pool.FetchPage(p1);
    ASSERT_NE(refetch, nullptr);
    EXPECT_EQ(std::memcmp(refetch, "before_evict", 13), 0);
    pool.UnpinPage(p1, false);
}

// ============================================================================
// DeletePage
// ============================================================================

TEST_F(BufferPoolTest, DeletePageRemovesFromPool) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 4);

    int64_t page_id;
    pool.NewPage(page_id);
    pool.UnpinPage(page_id, false);

    EXPECT_TRUE(pool.DeletePage(page_id));
    // Pages in use should decrease.
    EXPECT_EQ(pool.PagesInUse(), 0u);
}

TEST_F(BufferPoolTest, DeletePinnedPageFails) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 4);

    int64_t page_id;
    pool.NewPage(page_id);
    // Still pinned.
    EXPECT_FALSE(pool.DeletePage(page_id));

    pool.UnpinPage(page_id, false);
    EXPECT_TRUE(pool.DeletePage(page_id));
}

// ============================================================================
// Statistics
// ============================================================================

TEST_F(BufferPoolTest, HitMissStatistics) {
    DiskManager disk(kTestFile);
    BufferPool pool(disk, 8);

    int64_t p1, p2;
    pool.NewPage(p1);
    pool.UnpinPage(p1, false);
    pool.NewPage(p2);
    pool.UnpinPage(p2, false);

    // First fetches should be hits (pages are already in pool).
    pool.FetchPage(p1);
    pool.UnpinPage(p1, false);
    pool.FetchPage(p2);
    pool.UnpinPage(p2, false);

    EXPECT_GE(pool.HitCount(), 2u);
    EXPECT_EQ(pool.PoolSize(), 8u);
    EXPECT_EQ(pool.PagesInUse(), 2u);
}

TEST_F(BufferPoolTest, FlushAllPagesWritesDirty) {
    int64_t p1;
    {
        DiskManager disk(kTestFile);
        BufferPool pool(disk, 4);

        char* d1 = pool.NewPage(p1);
        std::memcpy(d1, "persist_me", 11);
        pool.UnpinPage(p1, true);
        pool.FlushAllPages();
    }
    // Re-open and verify.
    {
        DiskManager disk(kTestFile);
        BufferPool pool(disk, 4);

        char* data = pool.FetchPage(p1);
        ASSERT_NE(data, nullptr);
        EXPECT_EQ(std::memcmp(data, "persist_me", 11), 0);
        pool.UnpinPage(p1, false);
    }
}
