# Roadmap

A phased plan to evolve this project from a course assignment into a
portfolio-grade storage engine and eventually a mini database.

---

## Phase 1 — Foundation & Code Quality ✅

- [x] Modular project structure (`include/`, `src/`, `tests/`, `tools/`)
- [x] CMake build system with GoogleTest integration
- [x] Proper header / source separation
- [x] Namespace (`bptree`)
- [x] RAII throughout (DiskManager, BPlusTree)
- [x] `Status` error type (no more `exit(1)`)
- [x] Typed page wrappers (`LeafPage`, `InternalPage`)
- [x] DiskManager extracted from tree logic
- [x] Comprehensive unit tests (20+ test cases)
- [x] Cleaned-up interactive shell and benchmark tool

---

## Phase 2 — Storage Engine Hardening (in progress)

- [x] **Buffer Pool Manager** — LRU page cache (configurable frame count,
      default 1024 = 4 MB); pin / unpin semantics; dirty tracking;
      hit / miss statistics; tested (10 unit tests)
- [ ] **Write-Ahead Log (WAL)** — append-only log for crash recovery;
      redo-only protocol; checkpoint support
- [x] **Proper delete rebalancing** — redistribute from sibling when possible,
      otherwise merge; handles both leaf and internal underflow;
      root shrink when empty; tested (8 new tests including large-scale
      delete, alternating delete, delete-then-range, persistence)
- [ ] **Templated keys** — support `int`, `int64_t`, `std::string`, composite
      keys via `KeyComparator` trait
- [ ] **Variable-length records** — slotted page layout; overflow pages
- [ ] **Concurrency control** — reader-writer latches on pages; latch crabbing
      for safe concurrent tree traversal
- [x] **Free-page list** — singly-linked list through freed pages; reclaimed
      on next `AllocatePage`; integrated with buffer pool `DeletePage`

---

## Phase 3 — SQL Layer

- [ ] **Tokenizer / Lexer** — hand-written lexer for SQL subset
- [ ] **Parser** — recursive-descent parser producing an AST  
       Statements: `CREATE TABLE`, `INSERT`, `SELECT … WHERE`, `UPDATE`, `DELETE`
- [ ] **Catalog / schema manager** — store table metadata (column names, types)
      in a system table backed by the B+ tree
- [ ] **Query executor** — interpret the AST; sequential scan + index scan
- [ ] **Simple optimizer** — push-down predicates, use index when possible
- [ ] **Aggregate functions** — `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

---

## Phase 4 — Networking & Client

- [ ] **TCP server** — epoll-based event loop; accept concurrent connections
- [ ] **Wire protocol** — simple text or length-prefixed binary protocol
      (optionally a subset of the PostgreSQL wire protocol)
- [ ] **Client library** — C++ client that connects over TCP
- [ ] **CLI client** — `bptree-cli` that sends SQL over the wire to the server
- [ ] **Connection pooling** — limit concurrent connections; queue overflow

---

## Phase 5 — Polish & Extras

- [ ] **Tree visualizer** — DOT / Graphviz output of the B+ tree structure
- [ ] **Logging framework** — structured logging with severity levels
- [ ] **Metrics** — operation counts, latency histograms, cache hit rate
- [ ] **CI / CD** — GitHub Actions pipeline: build → test → benchmark
- [ ] **Doxygen docs** — auto-generated API docs from doc-comments
- [ ] **Fuzz testing** — AFL / libFuzzer to find crash bugs
- [ ] **Comparison benchmarks** — compare against SQLite, RocksDB, LevelDB

---

### Stretch Goals

| Goal                                | Why                                   |
| ----------------------------------- | ------------------------------------- |
| MVCC (multi-version concurrency)    | Snapshot isolation without read-locks |
| Join support (`INNER JOIN`)         | Core relational algebra               |
| B+ tree bulk-loading                | Much faster than one-by-one insert    |
| Compression (LZ4 / Snappy per page) | Reduce I/O                            |
| WASM build                          | Run the engine in the browser         |
