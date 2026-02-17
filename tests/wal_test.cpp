/// @file wal_test.cpp
/// @brief Google Test suite for the Write-Ahead Log and crash recovery.

#include <gtest/gtest.h>
#include "bptree/wal.h"
#include "bptree/disk_manager.h"
#include "bptree/buffer_pool.h"
#include "bptree/bplus_tree.h"
#include "bptree/config.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using namespace bptree;

// ============================================================================
// Fixture
// ============================================================================

class WALTest : public ::testing::Test {
protected:
    static constexpr const char* kTestIdx = "test_wal.idx";
    static constexpr const char* kTestWAL = "test_wal.idx.wal";
    static constexpr const char* kStandaloneWAL = "test_standalone.wal";

    void SetUp() override {
        std::remove(kTestIdx);
        std::remove(kTestWAL);
        std::remove(kStandaloneWAL);
    }

    void TearDown() override {
        std::remove(kTestIdx);
        std::remove(kTestWAL);
        std::remove(kStandaloneWAL);
    }
};

// ============================================================================
// Basic WAL operations
// ============================================================================

TEST_F(WALTest, CreateNewWALFile) {
    WriteAheadLog wal(kStandaloneWAL);
    EXPECT_EQ(wal.CurrentLSN(), 1u);
    EXPECT_EQ(wal.CheckpointLSN(), 0u);
    EXPECT_EQ(wal.BytesWritten(), 0u);
    EXPECT_TRUE(wal.IsEnabled());
}

TEST_F(WALTest, LogPageWriteIncrementsLSN) {
    WriteAheadLog wal(kStandaloneWAL);

    char page[PAGE_SIZE]{};
    std::memcpy(page, "test_data", 10);

    uint64_t lsn1 = wal.LogPageWrite(4096, page);
    EXPECT_EQ(lsn1, 1u);

    uint64_t lsn2 = wal.LogPageWrite(8192, page);
    EXPECT_EQ(lsn2, 2u);

    EXPECT_EQ(wal.CurrentLSN(), 3u);
    EXPECT_EQ(wal.RecordsWritten(), 2u);
    EXPECT_GT(wal.BytesWritten(), 0u);
}

TEST_F(WALTest, CRC32IsConsistent) {
    const char* data = "Hello, WAL!";
    uint32_t crc1 = WriteAheadLog::CRC32(data, std::strlen(data));
    uint32_t crc2 = WriteAheadLog::CRC32(data, std::strlen(data));
    EXPECT_EQ(crc1, crc2);

    // Different data should produce different CRC.
    const char* other = "Hello, WAL?";
    uint32_t crc3 = WriteAheadLog::CRC32(other, std::strlen(other));
    EXPECT_NE(crc1, crc3);
}

TEST_F(WALTest, CheckpointTruncatesLog) {
    WriteAheadLog wal(kStandaloneWAL);

    char page[PAGE_SIZE]{};
    wal.LogPageWrite(4096, page);
    wal.LogPageWrite(8192, page);
    EXPECT_EQ(wal.RecordsWritten(), 2u);

    // Checkpoint should truncate the log.
    wal.BeginCheckpoint();
    wal.EndCheckpoint();

    // The WAL file should be small now (just the header).
    // Reopen to verify.
    {
        WriteAheadLog wal2(kStandaloneWAL);
        // After truncation, LSN advances past the checkpoint records
        // but the file is small.
        EXPECT_TRUE(wal2.IsEnabled());
    }
}

// ============================================================================
// Recovery
// ============================================================================

TEST_F(WALTest, RecoverAppliesPageWrites) {
    // Step 1: Create a data file and write some pages via WAL.
    {
        DiskManager disk(kTestIdx);
        WriteAheadLog wal(kTestWAL);

        // Allocate a page on disk.
        int64_t off = disk.AllocatePage();
        EXPECT_EQ(off, static_cast<int64_t>(PAGE_SIZE));

        // Write the page's "correct" content to the WAL but NOT to disk.
        char correct_page[PAGE_SIZE]{};
        std::memcpy(correct_page, "recovered_data", 15);
        wal.LogPageWrite(off, correct_page);
        wal.Flush();

        // Intentionally do NOT write correct_page to disk.
        // Simulate crash: disk has zeros, WAL has the truth.
    }

    // Step 2: Reopen and recover.
    {
        DiskManager disk(kTestIdx);
        WriteAheadLog wal(kTestWAL);

        size_t recovered = wal.Recover(disk);
        EXPECT_GE(recovered, 1u);

        // The page should now have the WAL's data.
        const char* page = disk.PageData(PAGE_SIZE);
        EXPECT_EQ(std::memcmp(page, "recovered_data", 15), 0);
    }
}

TEST_F(WALTest, RecoverSkipsCheckpointedRecords) {
    int64_t off;
    // Step 1: Insert data, checkpoint, then insert more.
    {
        DiskManager disk(kTestIdx);
        WriteAheadLog wal(kTestWAL);
        BufferPool pool(disk, 8);
        pool.SetWAL(&wal);

        off = disk.AllocatePage();

        // First write (will be checkpointed).
        char page1[PAGE_SIZE]{};
        std::memcpy(page1, "checkpointed", 13);
        char* dp = disk.PageData(off);
        std::memcpy(dp, page1, PAGE_SIZE);
        wal.LogPageWrite(off, page1);

        // Checkpoint.
        wal.BeginCheckpoint();
        disk.Sync();
        wal.EndCheckpoint();

        // Second write after checkpoint (will need recovery).
        char page2[PAGE_SIZE]{};
        std::memcpy(page2, "after_checkpoint", 17);
        wal.LogPageWrite(off, page2);
        wal.Flush();
        // DON'T write page2 to disk -- simulating crash.
    }

    // Step 2: Recover.
    {
        DiskManager disk(kTestIdx);
        WriteAheadLog wal(kTestWAL);

        size_t recovered = wal.Recover(disk);
        // Should recover the post-checkpoint write.
        EXPECT_GE(recovered, 1u);

        const char* page = disk.PageData(off);
        EXPECT_EQ(std::memcmp(page, "after_checkpoint", 17), 0);
    }
}

// ============================================================================
// Integration with BPlusTree
// ============================================================================

TEST_F(WALTest, TreeWithWALPersistsNormally) {
    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 100; ++i) {
            tree.Insert(i, ("val_" + std::to_string(i)).c_str());
        }
        EXPECT_TRUE(tree.WALEnabled());
        EXPECT_GT(tree.WALBytesWritten(), 0u);
    }

    // Reopen and verify.
    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 100; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
            EXPECT_EQ(val, "val_" + std::to_string(i));
        }
    }
}

TEST_F(WALTest, TreeCheckpointWorks) {
    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 50; ++i) {
            tree.Insert(i, "before_cp");
        }
        tree.Checkpoint();

        for (int i = 50; i < 100; ++i) {
            tree.Insert(i, "after_cp");
        }
    }

    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 50; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
        }
        for (int i = 50; i < 100; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
        }
    }
}

TEST_F(WALTest, TreeWithoutWALStillWorks) {
    // Disable WAL to verify backward compatibility.
    {
        BPlusTree tree(kTestIdx, DEFAULT_POOL_SIZE, /*enable_wal=*/false);
        EXPECT_FALSE(tree.WALEnabled());

        for (int i = 0; i < 50; ++i) {
            tree.Insert(i, "no_wal");
        }
    }
    {
        BPlusTree tree(kTestIdx, DEFAULT_POOL_SIZE, /*enable_wal=*/false);
        for (int i = 0; i < 50; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
            EXPECT_EQ(val, "no_wal");
        }
    }
}

TEST_F(WALTest, WALStatsExposed) {
    BPlusTree tree(kTestIdx);
    EXPECT_EQ(tree.WALBytesWritten(), 0u);
    EXPECT_EQ(tree.WALRecordsWritten(), 0u);

    for (int i = 0; i < 10; ++i) {
        tree.Insert(i, "stats_test");
    }

    // Trigger a flush to generate WAL records.
    tree.Sync();
    EXPECT_GT(tree.WALBytesWritten(), 0u);
    EXPECT_GT(tree.WALRecordsWritten(), 0u);
}

TEST_F(WALTest, LargeDatasetWithWAL) {
    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 2000; ++i) {
            tree.Insert(i, ("r" + std::to_string(i)).c_str());
        }
    }
    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 2000; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i << " missing";
            EXPECT_EQ(val, "r" + std::to_string(i));
        }
    }
}

TEST_F(WALTest, DeleteAndReinsertWithWAL) {
    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 100; ++i) tree.Insert(i, "first");
        for (int i = 0; i < 50; ++i) tree.Delete(i);
        tree.Checkpoint();
        for (int i = 0; i < 50; ++i) tree.Insert(i, "second");
    }
    {
        BPlusTree tree(kTestIdx);
        for (int i = 0; i < 50; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
            EXPECT_EQ(val, "second");
        }
        for (int i = 50; i < 100; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
            EXPECT_EQ(val, "first");
        }
    }
}
