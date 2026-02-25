# B+ Tree Storage Engine Benchmarks

This document details the latency and throughput profile of the `BPlusSQL` storage engine. Testing covers point queries, range scans, inserts, and a mixed CRUD workload.

## Methodology
- **Specs:** Evaluated using the `/tools/bench` executable.
- **Keys / Data:** `int` keys (4 bytes) and fixed `100-byte` payloads.
- **Tree Params:** 4 KB page size, yielding ~35 records per leaf node and 100 children per internal node.
- **Disk I/O:** `mmap`-backed storage with `fsync` for WAL checkpointing.

---

## Results Snapshot

*Note: Results vary based on underlying hardware, OS caching, and available CPU/disk IOPs.*

```text
───────────────────────────────────────────────────────────────────────────────
 B+ Tree Storage Engine — Latency & Throughput Benchmark
───────────────────────────────────────────────────────────────────────────────

TEST 1: Sequential Insert (100,000 records)
  Insert(Seq)        3413.2 ops/s | Latency (us): min     1, avg  291.8, p99  3020, max   9901

TEST 2: Random Search (100,000 lookups)
  Search(Rand)      34212.1 ops/s | Latency (us): min     2, avg   28.6, p99    37, max   7172

TEST 3: Range Queries (10,000 queries, 0-100 elements each)
  RangeQuery        51956.7 ops/s | Latency (us): min     3, avg   17.9, p99    40, max   4003

TEST 4: Mixed Workload (100,000 ops)
  50% read · 30% insert · 10% range · 10% delete
  Mix: Insert       16568.8 ops/s | Latency (us): min     3, avg   60.4, p99  2209, max   4871
  Mix: Search        3144.6 ops/s | Latency (us): min     2, avg  318.0, p99  2856, max   8835
  Mix: Range          847.7 ops/s | Latency (us): min     3, avg 1179.6, p99  6764, max  48421
  Mix: Delete        1368.2 ops/s | Latency (us): min     4, avg  730.9, p99  5145, max  12047

  Total Mixed Time: 36943.7 ms (2706.8 ops/sec)

───────────────────────────────────────────────────────────────────────────────
OVERALL RUNTIME & STATS
───────────────────────────────────────────────────────────────────────────────

  Total Benchmark Time: 69357.6 ms
  Buffer pool hit rate: 82.5254%
  WAL bytes written:    114609792 B
  WAL records:          27764
```

## Observations
- **Reads vs. Writes:** Search operations scale well (~34K ops/sec standalone) due to high buffer pool hit rates (~82%) and zero-copy mmap. Writes are slower (~3.4K seq ops/sec) due to WAL persistence and B+ tree node splitting overhead.
- **Latency Spikes:** `p99` latencies in the mixed workload show periodic spikes, primarily during page splits and WAL checkpoints, leading to max tail latencies in the low tens of milliseconds.

To run the benchmark yourself:
```bash
./build/tools/bench
```
