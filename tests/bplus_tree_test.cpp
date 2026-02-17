/// @file bplus_tree_test.cpp
/// @brief Comprehensive Google Test suite for BPlusTree.

#include <gtest/gtest.h>
#include "bptree/bplus_tree.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <vector>

using namespace bptree;

/// Fixture that creates a fresh index file for each test and cleans up after.
class BPlusTreeTest : public ::testing::Test {
protected:
    static constexpr const char* kTestFile = "test_bptree.idx";

    void SetUp() override { std::remove(kTestFile); }

    void TearDown() override { std::remove(kTestFile); }

    /// Helper: create a tree with the test file.
    BPlusTree MakeTree() { return BPlusTree(kTestFile); }
};

// ============================================================================
// Basic CRUD
// ============================================================================

TEST_F(BPlusTreeTest, EmptyTreeIsEmpty) {
    auto tree = MakeTree();
    EXPECT_TRUE(tree.IsEmpty());
}

TEST_F(BPlusTreeTest, InsertMakesNonEmpty) {
    auto tree = MakeTree();
    ASSERT_TRUE(tree.Insert(1, "hello").ok());
    EXPECT_FALSE(tree.IsEmpty());
}

TEST_F(BPlusTreeTest, InsertAndSearch) {
    auto tree = MakeTree();
    ASSERT_TRUE(tree.Insert(42, "the answer").ok());

    std::string val;
    ASSERT_TRUE(tree.Search(42, val).ok());
    EXPECT_EQ(val, "the answer");
}

TEST_F(BPlusTreeTest, SearchMissReturnsNotFound) {
    auto tree = MakeTree();
    tree.Insert(1, "x");

    std::string val;
    Status s = tree.Search(999, val);
    EXPECT_TRUE(s.IsNotFound());
}

TEST_F(BPlusTreeTest, UpsertUpdatesExistingKey) {
    auto tree = MakeTree();
    tree.Insert(10, "version_1");
    tree.Insert(10, "version_2");

    std::string val;
    ASSERT_TRUE(tree.Search(10, val).ok());
    EXPECT_EQ(val, "version_2");
}

TEST_F(BPlusTreeTest, DeleteExistingKey) {
    auto tree = MakeTree();
    tree.Insert(5, "data");
    ASSERT_TRUE(tree.Delete(5).ok());

    std::string val;
    EXPECT_TRUE(tree.Search(5, val).IsNotFound());
}

TEST_F(BPlusTreeTest, DeleteNonExistentKeyReturnsNotFound) {
    auto tree = MakeTree();
    tree.Insert(1, "x");
    EXPECT_TRUE(tree.Delete(999).IsNotFound());
}

// ============================================================================
// Range queries
// ============================================================================

TEST_F(BPlusTreeTest, RangeQueryBasic) {
    auto tree = MakeTree();
    for (int i = 1; i <= 20; ++i) {
        tree.Insert(i, ("val_" + std::to_string(i)).c_str());
    }

    std::vector<std::pair<key_t, std::string>> results;
    ASSERT_TRUE(tree.RangeQuery(5, 10, results).ok());

    ASSERT_EQ(results.size(), 6u);
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(results[i].first, 5 + i);
    }
}

TEST_F(BPlusTreeTest, RangeQueryEmptyResult) {
    auto tree = MakeTree();
    tree.Insert(1, "x");

    std::vector<std::pair<key_t, std::string>> results;
    ASSERT_TRUE(tree.RangeQuery(100, 200, results).ok());
    EXPECT_TRUE(results.empty());
}

TEST_F(BPlusTreeTest, RangeQueryInvalidRange) {
    auto tree = MakeTree();
    std::vector<std::pair<key_t, std::string>> results;
    Status s = tree.RangeQuery(10, 5, results);
    EXPECT_FALSE(s.ok());
}

// ============================================================================
// Stress / split tests
// ============================================================================

TEST_F(BPlusTreeTest, InsertForcesLeafSplit) {
    auto tree = MakeTree();
    // LEAF_MAX_KEYS = 35, so 36 inserts triggers at least one split.
    for (int i = 1; i <= 50; ++i) {
        ASSERT_TRUE(tree.Insert(i, ("d" + std::to_string(i)).c_str()).ok());
    }
    // Verify all keys.
    for (int i = 1; i <= 50; ++i) {
        std::string val;
        ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i << " missing";
    }
}

TEST_F(BPlusTreeTest, InsertForcesInternalSplit) {
    auto tree = MakeTree();
    // Need enough inserts to fill an internal node (INTERNAL_MAX_KEYS = 100).
    // Each leaf holds 35 keys → ~3600 keys should create ~100+ leaves.
    const int N = 5000;
    for (int i = 0; i < N; ++i) {
        ASSERT_TRUE(tree.Insert(i, ("r" + std::to_string(i)).c_str()).ok());
    }
    for (int i = 0; i < N; ++i) {
        std::string val;
        ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i << " missing";
    }
}

TEST_F(BPlusTreeTest, RandomInsertAndSearch) {
    auto tree = MakeTree();

    std::mt19937 rng(12345);
    std::vector<int> keys(2000);
    std::iota(keys.begin(), keys.end(), 0);
    std::shuffle(keys.begin(), keys.end(), rng);

    for (int k : keys) {
        tree.Insert(k, ("v" + std::to_string(k)).c_str());
    }

    for (int k : keys) {
        std::string val;
        ASSERT_TRUE(tree.Search(k, val).ok()) << "key " << k << " missing";
        EXPECT_EQ(val, "v" + std::to_string(k));
    }
}

TEST_F(BPlusTreeTest, RangeQueryAfterSplits) {
    auto tree = MakeTree();
    for (int i = 0; i < 1000; ++i) {
        tree.Insert(i, ("d" + std::to_string(i)).c_str());
    }

    std::vector<std::pair<key_t, std::string>> results;
    ASSERT_TRUE(tree.RangeQuery(400, 600, results).ok());

    ASSERT_EQ(results.size(), 201u);
    for (int i = 0; i < 201; ++i) {
        EXPECT_EQ(results[i].first, 400 + i);
    }
}

// ============================================================================
// Persistence
// ============================================================================

TEST_F(BPlusTreeTest, DataPersistsAcrossReopen) {
    {
        auto tree = MakeTree();
        tree.Insert(1, "persistent_data");
    }  // tree is destroyed → file flushed

    {
        auto tree = MakeTree();
        std::string val;
        ASSERT_TRUE(tree.Search(1, val).ok());
        EXPECT_EQ(val, "persistent_data");
    }
}

TEST_F(BPlusTreeTest, LargeDatasetPersists) {
    const int N = 500;
    {
        auto tree = MakeTree();
        for (int i = 0; i < N; ++i) {
            tree.Insert(i, ("p" + std::to_string(i)).c_str());
        }
    }
    {
        auto tree = MakeTree();
        for (int i = 0; i < N; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
            EXPECT_EQ(val, "p" + std::to_string(i));
        }
    }
}

// ============================================================================
// Delete edge cases
// ============================================================================

TEST_F(BPlusTreeTest, DeleteAllKeys) {
    auto tree = MakeTree();
    for (int i = 0; i < 50; ++i) tree.Insert(i, "x");
    for (int i = 0; i < 50; ++i) ASSERT_TRUE(tree.Delete(i).ok());

    for (int i = 0; i < 50; ++i) {
        std::string val;
        EXPECT_TRUE(tree.Search(i, val).IsNotFound());
    }
}

TEST_F(BPlusTreeTest, DeleteThenReinsert) {
    auto tree = MakeTree();
    tree.Insert(42, "first");
    tree.Delete(42);
    tree.Insert(42, "second");

    std::string val;
    ASSERT_TRUE(tree.Search(42, val).ok());
    EXPECT_EQ(val, "second");
}

TEST_F(BPlusTreeTest, DeleteFromEmptyTree) {
    auto tree = MakeTree();
    EXPECT_TRUE(tree.Delete(1).IsNotFound());
}

// ============================================================================
// Delete rebalancing
// ============================================================================

TEST_F(BPlusTreeTest, DeleteCausesLeafUnderflow) {
    auto tree = MakeTree();
    // Insert enough keys to span multiple leaves, then delete from one.
    const int N = 100;
    for (int i = 0; i < N; ++i) tree.Insert(i, ("v" + std::to_string(i)).c_str());
    // Delete several keys from the low end to trigger underflow and rebalance.
    for (int i = 0; i < 30; ++i) ASSERT_TRUE(tree.Delete(i).ok());
    // Remaining keys should still be findable.
    for (int i = 30; i < N; ++i) {
        std::string val;
        ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i << " missing";
    }
    // Deleted keys should be gone.
    for (int i = 0; i < 30; ++i) {
        std::string val;
        EXPECT_TRUE(tree.Search(i, val).IsNotFound()) << "key " << i << " still present";
    }
}

TEST_F(BPlusTreeTest, DeleteCausesMultipleMerges) {
    auto tree = MakeTree();
    const int N = 500;
    for (int i = 0; i < N; ++i) tree.Insert(i, ("d" + std::to_string(i)).c_str());
    // Delete all keys in reverse order -- stresses right-merge / root shrink.
    for (int i = N - 1; i >= 0; --i) {
        ASSERT_TRUE(tree.Delete(i).ok()) << "delete key " << i << " failed";
    }
    EXPECT_TRUE(tree.IsEmpty());
}

TEST_F(BPlusTreeTest, DeleteAlternatingKeys) {
    auto tree = MakeTree();
    const int N = 200;
    for (int i = 0; i < N; ++i) tree.Insert(i, ("v" + std::to_string(i)).c_str());
    // Delete even keys.
    for (int i = 0; i < N; i += 2) {
        ASSERT_TRUE(tree.Delete(i).ok()) << "delete " << i;
    }
    // Odd keys remain.
    for (int i = 1; i < N; i += 2) {
        std::string val;
        ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i << " missing";
        EXPECT_EQ(val, "v" + std::to_string(i));
    }
}

TEST_F(BPlusTreeTest, DeleteThenRangeQuery) {
    auto tree = MakeTree();
    for (int i = 0; i < 100; ++i) tree.Insert(i, ("v" + std::to_string(i)).c_str());
    // Delete keys 20-39.
    for (int i = 20; i < 40; ++i) tree.Delete(i);
    // Range query 10-50 should skip deleted keys.
    std::vector<std::pair<key_t, std::string>> results;
    ASSERT_TRUE(tree.RangeQuery(10, 50, results).ok());
    // Should have keys 10-19, 40-50 = 10 + 11 = 21 results.
    EXPECT_EQ(results.size(), 21u);
}

TEST_F(BPlusTreeTest, DeleteAndReinsertLargeScale) {
    auto tree = MakeTree();
    const int N = 300;
    for (int i = 0; i < N; ++i) tree.Insert(i, "first");
    for (int i = 0; i < N; ++i) tree.Delete(i);
    EXPECT_TRUE(tree.IsEmpty());

    // Reinsert everything.
    for (int i = 0; i < N; ++i) tree.Insert(i, "second");
    for (int i = 0; i < N; ++i) {
        std::string val;
        ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
        EXPECT_EQ(val, "second");
    }
}

TEST_F(BPlusTreeTest, DeletePersistsAcrossReopen) {
    {
        auto tree = MakeTree();
        for (int i = 0; i < 50; ++i) tree.Insert(i, "data");
        for (int i = 0; i < 25; ++i) tree.Delete(i);
    }
    {
        auto tree = MakeTree();
        for (int i = 0; i < 25; ++i) {
            std::string val;
            EXPECT_TRUE(tree.Search(i, val).IsNotFound());
        }
        for (int i = 25; i < 50; ++i) {
            std::string val;
            ASSERT_TRUE(tree.Search(i, val).ok()) << "key " << i;
        }
    }
}

TEST_F(BPlusTreeTest, BufferPoolStatsAfterOperations) {
    auto tree = MakeTree();
    for (int i = 0; i < 100; ++i) tree.Insert(i, "x");
    for (int i = 0; i < 100; ++i) {
        std::string val;
        tree.Search(i, val);
    }
    // After repeated searches, hit rate should be positive.
    EXPECT_GT(tree.BufferPoolHits(), 0u);
    EXPECT_GT(tree.BufferPoolHitRate(), 0.0);
}
