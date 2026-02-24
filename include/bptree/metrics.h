/// @file metrics.h
/// @brief Atomic counters and latency tracking for B+ tree operations.

#pragma once

#include <atomic>
#include <chrono>

namespace bptree {

/// Latency statistics for a single operation type.
/// Uses atomics for thread-safe concurrent updates.
struct OpLatency {
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> total_us{0};
    std::atomic<uint64_t> max_us{0};

    void Record(uint64_t us) {
        count.fetch_add(1, std::memory_order_relaxed);
        total_us.fetch_add(us, std::memory_order_relaxed);
        
        uint64_t current_max = max_us.load(std::memory_order_relaxed);
        while (us > current_max) {
            if (max_us.compare_exchange_weak(current_max, us, std::memory_order_relaxed)) {
                break;
            }
        }
    }

    double AverageUs() const {
        uint64_t c = count.load(std::memory_order_relaxed);
        if (c == 0) return 0.0;
        return static_cast<double>(total_us.load(std::memory_order_relaxed)) / c;
    }
};

/// Collection of metrics for the B+ tree.
struct OpMetrics {
    OpLatency insert;
    OpLatency search;
    OpLatency remove; // using 'remove' instead of 'delete' (reserved keyword)
    OpLatency range;

    void Reset() {
        insert.count = 0; insert.total_us = 0; insert.max_us = 0;
        search.count = 0; search.total_us = 0; search.max_us = 0;
        remove.count = 0; remove.total_us = 0; remove.max_us = 0;
        range.count = 0; range.total_us = 0; range.max_us = 0;
    }
};

/// RAII helper to measure duration of a block and record it in OpLatency.
class ScopedTimer {
public:
    explicit ScopedTimer(OpLatency& stat)
        : stat_(stat), start_(std::chrono::high_resolution_clock::now()) {}

    ~ScopedTimer() {
        auto end = std::chrono::high_resolution_clock::now();
        uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
        stat_.Record(us);
    }

private:
    OpLatency& stat_;
    std::chrono::high_resolution_clock::time_point start_;
};

} // namespace bptree
