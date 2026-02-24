/// @file bench.cpp
/// @brief Detailed performance benchmark with ops/sec and min/avg/max/p99 latency.

#include "bptree/bplus_tree.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

using namespace bptree;
using Clock = std::chrono::high_resolution_clock;

static constexpr const char* kBenchFile = "bench.idx";

static void Sep() {
    std::cout << "───────────────────────────────────────────────────────────────────────────────\n";
}

class LatencyTracker {
public:
    void Reserve(size_t n) { latencies_.reserve(n); }
    void Record(uint64_t us) { latencies_.push_back(us); }

    struct Stats {
        size_t count;
        uint64_t min;
        uint64_t max;
        double avg;
        uint64_t p99;
    };

    Stats GetStats() {
        if (latencies_.empty()) return {0, 0, 0, 0.0, 0};
        std::sort(latencies_.begin(), latencies_.end());
        uint64_t min_us = latencies_.front();
        uint64_t max_us = latencies_.back();
        // p99 index: 99% of elements are below it
        size_t p99_idx = latencies_.size() * 99 / 100;
        if (p99_idx >= latencies_.size()) p99_idx = latencies_.size() - 1;
        uint64_t p99_us = latencies_[p99_idx];

        double sum = 0;
        for (auto l : latencies_) sum += l;

        return {latencies_.size(), min_us, max_us, sum / latencies_.size(), p99_us};
    }

private:
    std::vector<uint64_t> latencies_;
};

static void PrintStats(const char* name, double total_ms, LatencyTracker& tracker) {
    auto st = tracker.GetStats();
    double ops_sec = (st.count / total_ms) * 1000.0;
    std::printf("  %-15s %9.1f ops/s | Latency (us): min %5lu, avg %6.1f, p99 %5lu, max %6lu\n",
                name, ops_sec, st.min, st.avg, st.p99, st.max);
}

int main() {
    std::srand(static_cast<unsigned>(std::time(nullptr)));
    std::remove(kBenchFile);
    std::remove((std::string(kBenchFile) + ".wal").c_str());

    std::cout << "\n";
    Sep();
    std::cout << " B+ Tree Storage Engine — Latency & Throughput Benchmark\n";
    Sep();
    std::cout << "\n";

    BPlusTree tree(kBenchFile);

    // ── Test 1: Sequential Insert (100 K) ──────────────────────────────────
    std::cout << "TEST 1: Sequential Insert (100,000 records)\n";
    constexpr int N1 = 100'000;
    LatencyTracker tr_ins1;
    tr_ins1.Reserve(N1);

    auto t_start1 = Clock::now();
    for (int i = 0; i < N1; ++i) {
        char buf[DATA_SIZE]{};
        std::snprintf(buf, DATA_SIZE, "Record_%d_Data", i);
        
        auto op_start = Clock::now();
        tree.Insert(i, buf);
        tr_ins1.Record(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - op_start).count());
        
        if ((i + 1) % 25'000 == 0) std::cout << "  " << (i + 1) << " inserted...\r" << std::flush;
    }
    double ms1 = std::chrono::duration<double, std::milli>(Clock::now() - t_start1).count();
    std::cout << "                                \r";
    PrintStats("Insert(Seq)", ms1, tr_ins1);
    std::cout << "\n";

    // ── Test 2: Random Search (100 K) ───────────────────────────────────────
    std::cout << "TEST 2: Random Search (100,000 lookups)\n";
    constexpr int N2 = 100'000;
    LatencyTracker tr_srch2;
    tr_srch2.Reserve(N2);

    auto t_start2 = Clock::now();
    for (int i = 0; i < N2; ++i) {
        int key = std::rand() % N1;
        std::string val;
        
        auto op_start = Clock::now();
        tree.Search(key, val);
        tr_srch2.Record(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - op_start).count());
    }
    double ms2 = std::chrono::duration<double, std::milli>(Clock::now() - t_start2).count();
    PrintStats("Search(Rand)", ms2, tr_srch2);
    std::cout << "\n";

    // ── Test 3: Range Queries (10 K) ────────────────────────────────────────
    std::cout << "TEST 3: Range Queries (10,000 queries, 0-100 elements each)\n";
    constexpr int N3 = 10'000;
    LatencyTracker tr_rng3;
    tr_rng3.Reserve(N3);

    auto t_start3 = Clock::now();
    for (int i = 0; i < N3; ++i) {
        int lo = std::rand() % (N1 - 100);
        int hi = lo + std::rand() % 100;
        std::vector<std::pair<key_t, std::string>> res;
        
        auto op_start = Clock::now();
        tree.RangeQuery(lo, hi, res);
        tr_rng3.Record(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - op_start).count());
    }
    double ms3 = std::chrono::duration<double, std::milli>(Clock::now() - t_start3).count();
    PrintStats("RangeQuery", ms3, tr_rng3);
    std::cout << "\n";

    // ── Test 4: Mixed Workload (100 K ops) ──────────────────────────────────
    std::cout << "TEST 4: Mixed Workload (100,000 ops)\n"
              << "  50% read · 30% insert · 10% range · 10% delete\n";
    constexpr int N4 = 100'000;
    LatencyTracker tr_ins4, tr_r4, tr_rng4, tr_d4;
    
    int next_key = N1;
    auto t_start4 = Clock::now();
    for (int i = 0; i < N4; ++i) {
        int op = std::rand() % 100;
        if (op < 50) {
            std::string v; 
            auto s = Clock::now();
            tree.Search(std::rand() % next_key, v);
            tr_r4.Record(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - s).count());
        } else if (op < 80) {
            char buf[DATA_SIZE]{}; std::snprintf(buf, DATA_SIZE, "mix_%d", next_key);
            auto s = Clock::now();
            tree.Insert(next_key++, buf);
            tr_ins4.Record(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - s).count());
        } else if (op < 90) {
            int lo = std::rand() % (next_key - 50);
            std::vector<std::pair<key_t, std::string>> r;
            auto s = Clock::now();
            tree.RangeQuery(lo, lo + std::rand() % 50, r);
            tr_rng4.Record(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - s).count());
        } else {
            auto s = Clock::now();
            tree.Delete(std::rand() % next_key);
            tr_d4.Record(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - s).count());
        }
    }
    double ms4 = std::chrono::duration<double, std::milli>(Clock::now() - t_start4).count();
    
    double t_ins4 = (tr_ins4.GetStats().avg * tr_ins4.GetStats().count) / 1000.0;
    double t_r4 = (tr_r4.GetStats().avg * tr_r4.GetStats().count) / 1000.0;
    double t_rng4 = (tr_rng4.GetStats().avg * tr_rng4.GetStats().count) / 1000.0;
    double t_d4 = (tr_d4.GetStats().avg * tr_d4.GetStats().count) / 1000.0;

    PrintStats("Mix: Insert", t_ins4, tr_ins4);
    PrintStats("Mix: Search", t_r4, tr_r4);
    PrintStats("Mix: Range", t_rng4, tr_rng4);
    PrintStats("Mix: Delete", t_d4, tr_d4);

    std::printf("\n  Total Mixed Time: %.1f ms (%.1f ops/sec)\n\n", ms4, (N4 / ms4) * 1000.0);

    // ── Summary ────────────────────────────────────────────────────────────
    Sep();
    std::cout << "OVERALL RUNTIME & STATS\n";
    Sep();

    double total = ms1 + ms2 + ms3 + ms4;
    std::cout << "\n  Total Benchmark Time: " << total << " ms\n";
    std::cout << "  Buffer pool hit rate: " << (tree.BufferPoolHitRate() * 100) << "%\n";
    std::cout << "  WAL bytes written:    " << tree.WALBytesWritten() << " B\n";
    std::cout << "  WAL records:          " << tree.WALRecordsWritten() << "\n\n";

    std::remove(kBenchFile);
    std::remove((std::string(kBenchFile) + ".wal").c_str());
    return 0;
}
