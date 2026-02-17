/// @file disk_manager_test.cpp
/// @brief Google Test suite for DiskManager.

#include <gtest/gtest.h>
#include "bptree/disk_manager.h"
#include "bptree/config.h"

#include <cstdio>
#include <cstring>

using namespace bptree;

class DiskManagerTest : public ::testing::Test {
protected:
    static constexpr const char* kTestFile = "test_disk.idx";

    void SetUp() override { std::remove(kTestFile); }
    void TearDown() override { std::remove(kTestFile); }
};

TEST_F(DiskManagerTest, CreateNewFile) {
    DiskManager dm(kTestFile);
    EXPECT_TRUE(dm.IsValid());
    EXPECT_GE(dm.FileSize(), PAGE_SIZE);
}

TEST_F(DiskManagerTest, MetadataDefaults) {
    DiskManager dm(kTestFile);
    EXPECT_EQ(dm.RootOffset(), INVALID_PAGE_ID);
    EXPECT_EQ(dm.NextPageOffset(), static_cast<int64_t>(PAGE_SIZE));
}

TEST_F(DiskManagerTest, SetAndReadMetadata) {
    {
        DiskManager dm(kTestFile);
        dm.SetRootOffset(4096);
        dm.SetNextPageOffset(8192);
        dm.FlushMetadata();
    }
    {
        DiskManager dm(kTestFile);
        EXPECT_EQ(dm.RootOffset(), 4096);
        EXPECT_EQ(dm.NextPageOffset(), 8192);
    }
}

TEST_F(DiskManagerTest, AllocatePageReturnsValidOffset) {
    DiskManager dm(kTestFile);
    int64_t off1 = dm.AllocatePage();
    int64_t off2 = dm.AllocatePage();

    EXPECT_EQ(off1, static_cast<int64_t>(PAGE_SIZE));
    EXPECT_EQ(off2, static_cast<int64_t>(2 * PAGE_SIZE));
    EXPECT_GE(dm.FileSize(), static_cast<size_t>(3 * PAGE_SIZE));
}

TEST_F(DiskManagerTest, AllocatedPageIsZeroed) {
    DiskManager dm(kTestFile);
    int64_t off = dm.AllocatePage();
    const char* data = dm.PageData(off);

    for (size_t i = 0; i < PAGE_SIZE; ++i) {
        EXPECT_EQ(data[i], 0) << "byte " << i << " non-zero";
    }
}

TEST_F(DiskManagerTest, ReadWritePageData) {
    DiskManager dm(kTestFile);
    int64_t off = dm.AllocatePage();

    char* data = dm.PageData(off);
    std::strcpy(data, "hello from page");
    dm.Sync();

    const char* read = dm.PageData(off);
    EXPECT_STREQ(read, "hello from page");
}

TEST_F(DiskManagerTest, MultipleAllocations) {
    DiskManager dm(kTestFile);
    constexpr int N = 100;

    for (int i = 0; i < N; ++i) {
        int64_t off = dm.AllocatePage();
        char* page = dm.PageData(off);
        std::snprintf(page, PAGE_SIZE, "page_%d", i);
    }

    // Verify all pages have correct content.
    for (int i = 0; i < N; ++i) {
        int64_t off = static_cast<int64_t>((i + 1) * PAGE_SIZE);
        char expected[32];
        std::snprintf(expected, sizeof(expected), "page_%d", i);
        EXPECT_STREQ(dm.PageData(off), expected) << "page " << i;
    }
}

TEST_F(DiskManagerTest, OutOfRangePageThrows) {
    DiskManager dm(kTestFile);
    EXPECT_THROW((void)dm.PageData(999999), std::out_of_range);
    EXPECT_THROW((void)dm.PageData(-1), std::out_of_range);
}
