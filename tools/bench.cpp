/// @file bench.cpp
/// @brief Performance benchmark for the B+ tree storage engine.

#include "bptree/bplus_tree.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

using namespace bptree;
using Clock = std::chrono::high_resolution_clock;

static constexpr const char* kBenchFile = "bench.idx";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static void Sep() {
    std::cout << "────────────────────────────────────────────────\n";
}

template <typename D>
double Ms(D d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    std::srand(static_cast<unsigned>(std::time(nullptr)));
    std::remove(kBenchFile);

    std::cout << "\n";
    Sep();
    std::cout << " B+ Tree Storage Engine — Performance Benchmark\n";
    Sep();
    std::cout << "\n";

    BPlusTree tree(kBenchFile);

    // ── Test 1: Sequential Insert (100 K) ──────────────────────────────────

    Sep();
    std::cout << "TEST 1: Sequential Insert (100,000 records)\n";
    Sep();

    constexpr int N1 = 100'000;
    auto t0 = Clock::now();
    for (int i = 0; i < N1; ++i) {
        char buf[DATA_SIZE]{};
        std::snprintf(buf, DATA_SIZE, "Record_%d_Data", i);
        tree.Insert(i, buf);
        if ((i + 1) % 20'000 == 0)
            std::cout << "  " << (i + 1) << " inserted\n";
    }
    double ms1 = Ms(Clock::now() - t0);

    std::cout << "\n  Time:       " << ms1 << " ms\n"
              << "  Throughput: " << (N1 / ms1 * 1000) << " inserts/s\n\n";

    // ── Test 2: Random Search (10 K) ───────────────────────────────────────

    Sep();
    std::cout << "TEST 2: Random Search (10,000 lookups)\n";
    Sep();

    constexpr int N2 = 10'000;
    int hits = 0;
    t0 = Clock::now();
    for (int i = 0; i < N2; ++i) {
        std::string val;
        if (tree.Search(std::rand() % N1, val).ok()) ++hits;
    }
    double ms2 = Ms(Clock::now() - t0);

    std::cout << "\n  Time:       " << ms2 << " ms"
              << "  (" << hits << "/" << N2 << " hits)\n"
              << "  Throughput: " << (N2 / ms2 * 1000) << " searches/s\n\n";

    // ── Test 3: Range Queries (100) ────────────────────────────────────────

    Sep();
    std::cout << "TEST 3: Range Queries (100 queries)\n";
    Sep();

    int total_records = 0;
    t0 = Clock::now();
    for (int i = 0; i < 100; ++i) {
        int lo = std::rand() % 99'000;
        int hi = lo + std::rand() % 1000;
        std::vector<std::pair<key_t, std::string>> res;
        tree.RangeQuery(lo, hi, res);
        total_records += static_cast<int>(res.size());
    }
    double ms3 = Ms(Clock::now() - t0);

    std::cout << "\n  Time:       " << ms3 << " ms"
              << "  (" << total_records << " total records)\n"
              << "  Throughput: " << (100.0 / ms3 * 1000) << " queries/s\n\n";

    // ── Test 4: Mixed Workload (10 K ops) ──────────────────────────────────

    Sep();
    std::cout << "TEST 4: Mixed Workload\n"
              << "  40 % read · 30 % write · 20 % range · 10 % delete\n";
    Sep();

    int next_key = N1;
    int ops_r = 0, ops_w = 0, ops_q = 0, ops_d = 0;
    t0 = Clock::now();
    for (int i = 0; i < 10'000; ++i) {
        int op = std::rand() % 100;
        if (op < 40) {
            std::string v; tree.Search(std::rand() % next_key, v); ++ops_r;
        } else if (op < 70) {
            char buf[DATA_SIZE]{}; std::snprintf(buf, DATA_SIZE, "mix_%d", next_key);
            tree.Insert(next_key++, buf); ++ops_w;
        } else if (op < 90) {
            int lo = std::rand() % (next_key - 100);
            std::vector<std::pair<key_t, std::string>> r;
            tree.RangeQuery(lo, lo + std::rand() % 100, r); ++ops_q;
        } else {
            tree.Delete(std::rand() % next_key); ++ops_d;
        }
    }
    double ms4 = Ms(Clock::now() - t0);

    std::cout << "\n  Time:       " << ms4 << " ms\n"
              << "  Reads: " << ops_r << "  Writes: " << ops_w
              << "  Ranges: " << ops_q << "  Deletes: " << ops_d << "\n"
              << "  Throughput: " << (10000.0 / ms4 * 1000) << " ops/s\n\n";

    // ── Summary ────────────────────────────────────────────────────────────

    Sep();
    std::cout << "SUMMARY\n";
    Sep();

    double total = ms1 + ms2 + ms3 + ms4;
    std::cout << "\n  Total: " << total << " ms\n";
    std::cout << "  Buffer pool hit rate: " << (tree.BufferPoolHitRate() * 100) << "%\n";
    if (tree.WALEnabled()) {
        std::cout << "  WAL bytes written:   " << tree.WALBytesWritten() << "\n";
        std::cout << "  WAL records:         " << tree.WALRecordsWritten() << "\n";
    }
    std::cout << "\n";

    auto pct = [&](double v) { return v / total * 100; };
    std::printf("  %-26s %8.0f ms  (%4.1f%%)\n", "Sequential Insert", ms1, pct(ms1));
    std::printf("  %-26s %8.0f ms  (%4.1f%%)\n", "Random Search",     ms2, pct(ms2));
    std::printf("  %-26s %8.0f ms  (%4.1f%%)\n", "Range Queries",     ms3, pct(ms3));
    std::printf("  %-26s %8.0f ms  (%4.1f%%)\n", "Mixed Workload",    ms4, pct(ms4));

    std::cout << "\n  Verdict: ";
    if (total < 3000)      std::cout << "EXCELLENT";
    else if (total < 5000) std::cout << "VERY GOOD";
    else if (total < 10000)std::cout << "GOOD";
    else                   std::cout << "NEEDS OPTIMIZATION";
    std::cout << "\n\n";

    std::remove(kBenchFile);
    return 0;
}
