/// @file concurrency_test.cpp
/// @brief Tests for concurrent reads and writes to the B+ tree.
///
/// We used to say "Not thread-safe" in the docs.  These tests are proof
/// we lied (in a good way) -- the tree is now thread-safe via latch crabbing.
/// Think of it as a bug that became a feature. :)

#include "bptree/bplus_tree.h"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// ============================================================================
// Test fixture
// ============================================================================

class ConcurrencyTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = "conc_test.idx";
        // Remove any leftover from a previous run.
        fs::remove(path_);
        fs::remove(path_ + ".wal");
    }

    void TearDown() override {
        fs::remove(path_);
        fs::remove(path_ + ".wal");
    }

    std::string path_;
};

// ============================================================================
// Helper
// ============================================================================

static std::string make_val(int key) {
    return "value_" + std::to_string(key);
}

// ============================================================================
// Tests
// ============================================================================

/// Many threads each insert a disjoint set of keys. At the end every key
/// must be found exactly once.
TEST_F(ConcurrencyTest, ConcurrentInsertsFromMultipleThreads) {
    bptree::BPlusTree tree(path_);

    const int   num_threads  = 8;
    const int   keys_per_thr = 100;

    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        workers.emplace_back([&tree, t]() {
            int base = t * keys_per_thr;
            for (int k = base; k < base + keys_per_thr; ++k) {
                std::string val = make_val(k);
                // Insert must not crash and must succeed.
                auto s = tree.Insert(k, val.c_str());
                EXPECT_TRUE(s.ok()) << "Insert(" << k << ") failed: " << s.ToString();
            }
        });
    }

    for (auto& w : workers) w.join();

    // All keys must be searchable after threads complete.
    int total = num_threads * keys_per_thr;
    for (int k = 0; k < total; ++k) {
        std::string val;
        auto s = tree.Search(k, val);
        EXPECT_TRUE(s.ok()) << "Search(" << k << ") not found after concurrent insert";
    }
}

/// One writer and many readers running at the same time.
/// Readers search keys the writer has already committed.
/// No crash == correctness here (we cannot check read consistency
/// of in-flight writes without MVCC, which is a stretch goal). :)
TEST_F(ConcurrencyTest, ConcurrentReadersAndOneWriter) {
    bptree::BPlusTree tree(path_);

    // Pre-populate 200 keys so readers have something to find right away.
    const int pre_keys = 200;
    for (int k = 0; k < pre_keys; ++k) {
        tree.Insert(k, make_val(k).c_str());
    }

    std::atomic<bool> stop{false};

    // Writer keeps inserting new keys beyond the pre-populated range.
    std::thread writer([&]() {
        int k = pre_keys;
        while (!stop.load()) {
            tree.Insert(k, make_val(k).c_str());
            ++k;
        }
    });

    // Readers keep searching pre-populated keys -- they must always be found.
    const int num_readers = 4;
    std::vector<std::thread> readers;
    readers.reserve(num_readers);

    for (int r = 0; r < num_readers; ++r) {
        readers.emplace_back([&]() {
            for (int iter = 0; iter < 500; ++iter) {
                int k = iter % pre_keys;
                std::string val;
                auto s = tree.Search(k, val);
                EXPECT_TRUE(s.ok()) << "Reader lost key " << k;
            }
        });
    }

    for (auto& rd : readers) rd.join();
    stop.store(true);
    writer.join();
}

/// Many threads each delete a disjoint set of keys from a pre-filled tree.
/// No double-free or crash expected.
TEST_F(ConcurrencyTest, ConcurrentDeletes) {
    bptree::BPlusTree tree(path_);

    const int total_keys = 400;
    for (int k = 0; k < total_keys; ++k) {
        tree.Insert(k, make_val(k).c_str());
    }

    const int num_threads  = 4;
    const int keys_per_thr = total_keys / num_threads;

    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        workers.emplace_back([&tree, t, keys_per_thr]() {
            int base = t * keys_per_thr;
            for (int k = base; k < base + keys_per_thr; ++k) {
                auto s = tree.Delete(k);
                EXPECT_TRUE(s.ok()) << "Delete(" << k << ") failed";
            }
        });
    }

    for (auto& w : workers) w.join();

    // All deleted keys must be gone.
    for (int k = 0; k < total_keys; ++k) {
        std::string val;
        auto s = tree.Search(k, val);
        EXPECT_FALSE(s.ok()) << "Key " << k << " still found after concurrent delete";
    }
}

/// Concurrent range queries should not crash or return garbage.
TEST_F(ConcurrencyTest, ConcurrentRangeQueries) {
    bptree::BPlusTree tree(path_);

    const int keys = 300;
    for (int k = 0; k < keys; ++k) {
        tree.Insert(k, make_val(k).c_str());
    }

    const int num_threads = 6;
    std::vector<std::thread> readers;
    readers.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        readers.emplace_back([&tree, t]() {
            int lo = t * 10;
            int hi = lo + 50;
            for (int iter = 0; iter < 100; ++iter) {
                std::vector<std::pair<int, std::string>> results;
                auto s = tree.RangeQuery(lo, hi, results);
                EXPECT_TRUE(s.ok());
                // Each result key must be within [lo, hi].
                for (auto& [k, v] : results) {
                    EXPECT_GE(k, lo);
                    EXPECT_LE(k, hi);
                }
            }
        });
    }

    for (auto& rd : readers) rd.join();
}
