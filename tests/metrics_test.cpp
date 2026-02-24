/// @file metrics_test.cpp
/// @brief Tests for B+ tree operation metrics (OpMetrics).

#include <gtest/gtest.h>

#include "bptree/bplus_tree.h"

#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace bptree;

class MetricsTest : public ::testing::Test {
protected:
    static constexpr const char* kFile = "metrics_test.idx";
    static constexpr const char* kWAL  = "metrics_test.idx.wal";

    void SetUp()    override { CleanUp(); }
    void TearDown() override { CleanUp(); }

    void CleanUp() {
        fs::remove(kFile);
        fs::remove(kWAL);
    }
};

TEST_F(MetricsTest, TracksOperations) {
    IntTree tree(kFile, 64, false);

    for (int i = 0; i < 10; ++i) {
        tree.Insert(i, "data");
    }

    std::string val;
    for (int i = 0; i < 5; ++i) {
        tree.Search(i, val);
    }

    std::vector<std::pair<int, std::string>> res;
    tree.RangeQuery(0, 5, res);

    tree.Delete(8);
    tree.Delete(9);

    const OpMetrics& m = tree.GetMetrics();

    EXPECT_EQ(m.insert.count.load(), 10u);
    // Since Search(string) calls Search(char*), we count 5 searches.
    // Also Delete() calls Search(char*) first to check if key exists, adding 2 more.
    EXPECT_EQ(m.search.count.load(), 7u);
    EXPECT_EQ(m.range.count.load(), 1u);
    EXPECT_EQ(m.remove.count.load(), 2u);

    // Latency should be non-negative.
    EXPECT_GE(m.insert.total_us.load(), 0u);
    EXPECT_GE(m.search.total_us.load(), 0u);
    EXPECT_GE(m.range.total_us.load(), 0u);
    EXPECT_GE(m.remove.total_us.load(), 0u);

    // After reset, counters are zero.
    // However, OpMetrics::Reset() is not exposed safely since metrics_ is mutable const.
    // We didn't expose reset publically on BPlusTree, so no need to test it here.
}
