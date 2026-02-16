# Architecture

High-level overview of the storage engine's design.

## Layer Diagram

```
┌─────────────────────────────────────────────┐
│                  Client                     │
│         (shell, bench, test, SQL*)          │
├─────────────────────────────────────────────┤
│               BPlusTree                     │
│   Insert · Search · Delete · RangeQuery     │
├─────────────────────────────────────────────┤
│          Page Abstraction Layer             │
│       LeafPage  ·  InternalPage             │
├─────────────────────────────────────────────┤
│        Buffer Pool Manager (Phase 2)        │
│          LRU eviction · pin/unpin           │
├─────────────────────────────────────────────┤
│             DiskManager                     │
│      mmap · page allocation · sync          │
├─────────────────────────────────────────────┤
│           Linux Kernel / FS                 │
│          (ext4 / xfs / tmpfs)               │
└─────────────────────────────────────────────┘
```

## Component Details

### DiskManager (`include/bptree/disk_manager.h`)

Manages a single memory-mapped index file.

- **File I/O**: `open()` → `mmap()` → `munmap()` lifecycle via RAII.
- **Capacity growth**: doubles the file via `ftruncate` + re-`mmap` when the
  next allocated page would exceed the current mapping.
- **Page allocation**: returns a byte offset into the mapped region; newly
  allocated pages are zeroed.
- **Metadata page** (page 0): stores `root_offset` and `next_page_offset`.

### Page Wrappers (`include/bptree/page.h`)

Zero-overhead typed views over raw `char*` page buffers.

| Class          | Layout                                                      |
| -------------- | ----------------------------------------------------------- |
| `LeafPage`     | `[num_keys(4) \| is_leaf=1(4) \| next_leaf(8) \| records…]` |
| `InternalPage` | `[num_keys(4) \| is_leaf=0(4) \| slots…]`                   |

Each leaf record: `[key(4 B) \| data(100 B)]` = 104 bytes × 35 = 3640 B + 16 B header = 3656 B (fits in 4096 B page).

Each internal slot: `[child_ptr(8 B) \| key(4 B)]` = 12 bytes × 101 = 1212 B + 8 B header = 1220 B.

### BPlusTree (`include/bptree/bplus_tree.h`)

The core index.

- **Insert**: recursive descent to the target leaf → insert in sorted order →
  split upward if necessary → create new root on root split.
- **Search**: traverse internal nodes → binary-ish scan in the leaf.
- **Range Query**: locate starting leaf → follow `next_leaf` linked list →
  collect matching records.
- **Delete**: locate leaf → shift records left. _(Rebalancing planned for
  Phase 2.)_

### Status (`include/bptree/status.h`)

Lightweight result type inspired by LevelDB. Avoids `exit(1)` or exceptions
for expected errors like "key not found".

## File Format

```
┌────────────────────┐  offset 0
│  Metadata Page     │  root_offset (int64) + next_page_off (int64)
├────────────────────┤  offset 4096
│  Page 1            │  leaf or internal node
├────────────────────┤  offset 8192
│  Page 2            │
├────────────────────┤
│        ...         │
└────────────────────┘
```

The file grows in 4 KB increments. All writes go through `mmap` (MAP_SHARED),
so the kernel handles write-back to disk. `msync` is called on metadata
changes and periodically via `SyncAsync()`.

## Future: Buffer Pool Manager

```
BPlusTree  ──→  BufferPool  ──→  DiskManager
                 ↕ LRU cache of Page frames
                 ↕ pin count tracking
                 ↕ dirty flag → flush on eviction
```

The buffer pool will sit between the tree and the disk manager, providing:

- Fixed-size pool of in-memory page frames
- LRU eviction of unpinned pages
- Dirty-page tracking and write-back
- Foundation for WAL integration
