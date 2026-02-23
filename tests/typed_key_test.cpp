/// @file typed_key_test.cpp
/// @brief Tests for BPlusTree<int64_t> and BPlusTree<std::string> --
///        proving the template works beyond just int keys.
///
/// If these pass we can officially say Phase 2 templated keys are done. :)

#include "bptree/bplus_tree.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// ============================================================================
// int64_t key tests
// ============================================================================

class BigIntTreeTest : public ::testing::Test {
protected:
    void SetUp()    override { CleanUp(); }
    void TearDown() override { CleanUp(); }

    void CleanUp() {
        fs::remove(path_);
        fs::remove(path_ + ".wal");
    }

    const std::string path_ = "bigint_test.idx";
};

TEST_F(BigIntTreeTest, InsertAndSearch) {
    bptree::BigIntTree tree(path_);
    ASSERT_TRUE(tree.Insert(100'000'000'000LL, "big value").ok());

    std::string val;
    ASSERT_TRUE(tree.Search(100'000'000'000LL, val).ok());
    EXPECT_EQ(val, "big value");
}

TEST_F(BigIntTreeTest, NegativeKeys) {
    bptree::BigIntTree tree(path_);
    tree.Insert(-9999LL, "negative");
    tree.Insert( 0LL,    "zero");
    tree.Insert( 9999LL, "positive");

    std::string val;
    ASSERT_TRUE(tree.Search(-9999LL, val).ok());
    EXPECT_EQ(val, "negative");
}

TEST_F(BigIntTreeTest, RangeQueryInt64) {
    bptree::BigIntTree tree(path_);
    for (int64_t k = 0; k < 50; ++k) {
        tree.Insert(k, ("v" + std::to_string(k)).c_str());
    }

    std::vector<std::pair<int64_t, std::string>> res;
    ASSERT_TRUE(tree.RangeQuery(10LL, 20LL, res).ok());
    ASSERT_EQ(res.size(), 11u);
    EXPECT_EQ(res.front().first, 10LL);
    EXPECT_EQ(res.back().first,  20LL);
}

TEST_F(BigIntTreeTest, DeleteAndRangeQuery) {
    bptree::BigIntTree tree(path_);
    for (int64_t k = 0; k < 100; ++k) tree.Insert(k, "data");
    for (int64_t k = 0; k < 50;  ++k) ASSERT_TRUE(tree.Delete(k).ok());

    std::vector<std::pair<int64_t, std::string>> res;
    ASSERT_TRUE(tree.RangeQuery(0LL, 99LL, res).ok());
    EXPECT_EQ(res.size(), 50u);
    EXPECT_EQ(res.front().first, 50LL);
}

// ============================================================================
// std::string key tests
// ============================================================================

class StrTreeTest : public ::testing::Test {
protected:
    void SetUp()    override { CleanUp(); }
    void TearDown() override { CleanUp(); }

    void CleanUp() {
        fs::remove(path_);
        fs::remove(path_ + ".wal");
    }

    const std::string path_ = "str_test.idx";
};

TEST_F(StrTreeTest, InsertAndSearch) {
    bptree::StrTree tree(path_);
    ASSERT_TRUE(tree.Insert("hello", "world").ok());

    std::string val;
    ASSERT_TRUE(tree.Search("hello", val).ok());
    EXPECT_EQ(val, "world");
}

TEST_F(StrTreeTest, LexicographicOrder) {
    bptree::StrTree tree(path_);
    tree.Insert("banana", "b");
    tree.Insert("apple",  "a");
    tree.Insert("cherry", "c");

    // Range query should return in lexicographic order.
    std::vector<std::pair<std::string, std::string>> res;
    ASSERT_TRUE(tree.RangeQuery("apple", "cherry", res).ok());
    ASSERT_EQ(res.size(), 3u);
    EXPECT_EQ(res[0].first, "apple");
    EXPECT_EQ(res[1].first, "banana");
    EXPECT_EQ(res[2].first, "cherry");
}

TEST_F(StrTreeTest, ManyStringKeyInserts) {
    bptree::StrTree tree(path_);
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        tree.Insert(key, ("val_" + std::to_string(i)).c_str());
    }
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string val;
        ASSERT_TRUE(tree.Search(key, val).ok()) << "key " << key << " missing";
    }
}

TEST_F(StrTreeTest, DeleteStringKey) {
    bptree::StrTree tree(path_);
    tree.Insert("foo", "bar");
    ASSERT_TRUE(tree.Delete("foo").ok());

    std::string val;
    EXPECT_TRUE(tree.Search("foo", val).IsNotFound());
}
