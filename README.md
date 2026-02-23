# bptree-db

A disk-based **B+ tree storage engine** built from scratch in modern C++17.
Uses memory-mapped I/O for zero-copy page access and delivers persistent,
sorted key-value storage with efficient range queries.

> Originally a DBMS course project, now being evolved into a full storage
> engine with SQL support. See the [Roadmap](docs/ROADMAP.md) for the plan.

---

## Features

| Feature                                               | Status         |
| ----------------------------------------------------- | -------------- |
| Disk-persistent B+ tree with 4 KB pages               | done           |
| Memory-mapped I/O (mmap) for zero-copy reads          | done           |
| Insert / upsert / point lookup / range query / delete | done           |
| Automatic node splitting on overflow                  | done           |
| Leaf linked-list for fast range scans                 | done           |
| Delete rebalancing (redistribute + merge)             | done           |
| Free-page list (recycled disk space)                  | done           |
| Clean separation: DiskManager -> BufferPool -> Tree   | done           |
| Status error type (no exit(1))                        | done           |
| Google Test suite (40+ tests)                         | done           |
| Interactive CLI shell                                 | done           |
| Performance benchmark tool                            | done           |
| Buffer pool manager (LRU, pin/unpin, hit/miss stats)  | done           |
| Write-ahead log (WAL, CRC32, checkpoint/recovery)     | done           |
| Structured logging (severity levels, file + console)  | done           |
| Tree visualizer (DOT/Graphviz + ASCII)                | done           |
| Concurrency control (reader-writer latches, crabbing) | done           |
| Templated keys (int, int64_t, std::string)            | done           |
| Variable-length records (slotted pages)               | Phase 2        |
| SQL parser and executor                               | Phase 3        |
| TCP server                                            | Phase 4        |

## Architecture

```
Client (shell / bench / tests)
        |
        v
   +-----------+    Insert, Search, Delete, RangeQuery
   | BPlusTree |
   +-----+-----+
         |  uses page wrappers (LeafPage, InternalPage)
         v
   +-----------+    LRU cache, pin/unpin, dirty tracking
   | BufferPool|
   +-----+-----+
         |
   +-----+--------+
   |              |
   v              v
  WAL         DiskManager    mmap, page alloc, sync, free-page list
   |              |
   +--------------+
         |
         v
   [ index file ]   (4 KB pages, grows dynamically)
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.

## Quick Start

### Prerequisites

- Linux (tested on Ubuntu 22.04+)
- g++ 9+ or clang++ 10+ with C++17 support
- CMake 3.14+
- Git (for GoogleTest fetch)

### Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run Tests

```bash
cd build
ctest --output-on-failure
```

### Run Benchmark

```bash
./build/tools/bench
```

### Interactive Shell

```bash
./build/tools/shell
```

## API

```cpp
#include "bptree/bplus_tree.h"

bptree::BPlusTree tree("my_index.idx");

// Insert
tree.Insert(42, "hello world");

// Point lookup
std::string value;
if (tree.Search(42, value).ok()) {
    std::cout << value << std::endl;
}

// Range query
std::vector<std::pair<int, std::string>> results;
tree.RangeQuery(10, 50, results);

// Delete
tree.Delete(42);

// Buffer pool stats
std::cout << "Hit rate: " << tree.BufferPoolHitRate() << std::endl;

// WAL checkpoint
tree.Checkpoint();
```

### Key Types and Sizes

| Parameter         | Value             |
| ----------------- | ----------------- |
| Page size         | 4096 bytes        |
| Record payload    | 100 bytes (fixed) |
| Key type          | int (32-bit)      |
| Leaf capacity     | 35 records / node |
| Internal capacity | 100 keys / node   |

### Complexity

| Operation   | Time         |
| ----------- | ------------ |
| Insert      | O(log n)     |
| Search      | O(log n)     |
| Range Query | O(log n + k) |
| Delete      | O(log n)     |

## Project Structure

```
CMakeLists.txt              -- Root build configuration
include/bptree/
    config.h                -- Constants and type aliases
    status.h                -- Error handling type
    page.h                  -- LeafPage and InternalPage wrappers
    disk_manager.h          -- Memory-mapped file manager
    buffer_pool.h           -- LRU page cache
    wal.h                   -- Write-ahead log
    logger.h                -- Structured logger (header-only)
    visualizer.h            -- DOT/ASCII tree visualizer
    bplus_tree.h            -- B+ tree public API
src/
    disk_manager.cpp        -- DiskManager implementation
    buffer_pool.cpp         -- BufferPool implementation
    wal.cpp                 -- WAL implementation
    bplus_tree.cpp          -- B+ tree implementation
    visualizer.cpp          -- Visualizer implementation
tests/
    bplus_tree_test.cpp     -- B+ tree unit tests
    buffer_pool_test.cpp    -- Buffer pool unit tests
    disk_manager_test.cpp   -- DiskManager unit tests
    logger_test.cpp         -- Logger unit tests
    wal_test.cpp            -- WAL unit tests
tools/
    shell.cpp               -- Interactive CLI
    bench.cpp               -- Performance benchmark
    visualize.cpp           -- Standalone visualizer tool
docs/
    ARCHITECTURE.md         -- Design documentation
    ROADMAP.md              -- Phased development plan
```

## Roadmap

| Phase | Focus                                             | Status     |
| ----- | ------------------------------------------------- | ---------- |
| 1     | Code quality, modular architecture, testing       | done       |
| 2     | Buffer pool, WAL, concurrency, delete rebalancing | in progress|
| 3     | SQL tokenizer, parser, executor                   | planned    |
| 4     | TCP server, wire protocol, client library         | planned    |
| 5     | Logging, metrics, CI/CD, fuzz testing             | planned    |

Full details: [docs/ROADMAP.md](docs/ROADMAP.md)

## License

MIT
