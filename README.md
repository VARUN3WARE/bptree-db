# bptree-db

A disk-based **B+ tree storage engine** built from scratch in modern C++17.
Uses memory-mapped I/O for zero-copy page access and delivers persistent,
sorted key-value storage with efficient range queries.

> Originally a DBMS course project, now being evolved into a full storage
> engine with SQL support. See the [Roadmap](docs/ROADMAP.md) for the plan.

---

## Features

| Feature                                               | Status     |
| ----------------------------------------------------- | ---------- |
| Disk-persistent B+ tree with 4 KB pages               | ✅         |
| Memory-mapped I/O (`mmap`) for zero-copy reads        | ✅         |
| Insert / upsert / point lookup / range query / delete | ✅         |
| Automatic node splitting on overflow                  | ✅         |
| Leaf linked-list for fast range scans                 | ✅         |
| Clean separation: DiskManager → Page → BPlusTree      | ✅         |
| `Status` error type (no `exit(1)`)                    | ✅         |
| Google Test suite (20+ tests)                         | ✅         |
| Interactive CLI shell                                 | ✅         |
| Performance benchmark tool                            | ✅         |
| Buffer pool manager                                   | 🔜 Phase 2 |
| Write-ahead log (WAL)                                 | 🔜 Phase 2 |
| Concurrency control                                   | 🔜 Phase 2 |
| SQL parser & executor                                 | 🔜 Phase 3 |
| TCP server                                            | 🔜 Phase 4 |

## Architecture

```
Client (shell / bench / tests)
        │
        ▼
   ┌──────────┐    Insert, Search, Delete, RangeQuery
   │ BPlusTree│
   └────┬─────┘
        │  uses page wrappers (LeafPage, InternalPage)
        ▼
   ┌──────────┐    mmap, page alloc, sync
   │DiskManager│
   └────┬─────┘
        │
        ▼
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
```

### Key Types & Sizes

| Parameter         | Value             |
| ----------------- | ----------------- |
| Page size         | 4096 bytes        |
| Record payload    | 100 bytes (fixed) |
| Key type          | `int` (32-bit)    |
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
├── CMakeLists.txt              # Root build configuration
├── include/bptree/
│   ├── config.h                # Constants & type aliases
│   ├── status.h                # Error handling type
│   ├── page.h                  # LeafPage & InternalPage wrappers
│   ├── disk_manager.h          # Memory-mapped file manager
│   └── bplus_tree.h            # B+ tree public API
├── src/
│   ├── disk_manager.cpp        # DiskManager implementation
│   └── bplus_tree.cpp          # B+ tree implementation
├── tests/
│   ├── bplus_tree_test.cpp     # B+ tree unit tests
│   └── disk_manager_test.cpp   # DiskManager unit tests
├── tools/
│   ├── shell.cpp               # Interactive CLI
│   └── bench.cpp               # Performance benchmark
└── docs/
    ├── ARCHITECTURE.md         # Design documentation
    └── ROADMAP.md              # Phased development plan
```

## Roadmap

| Phase | Focus                                             | Status     |
| ----- | ------------------------------------------------- | ---------- |
| **1** | Code quality, modular architecture, testing       | ✅ Done    |
| **2** | Buffer pool, WAL, concurrency, delete rebalancing | 🔧 Next    |
| **3** | SQL tokenizer, parser, executor                   | 📋 Planned |
| **4** | TCP server, wire protocol, client library         | 📋 Planned |
| **5** | Logging, metrics, CI/CD, fuzz testing             | 📋 Planned |

Full details: [docs/ROADMAP.md](docs/ROADMAP.md)

## License

MIT
