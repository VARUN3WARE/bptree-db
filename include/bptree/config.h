#pragma once

/// @file config.h
/// @brief Core constants and type definitions for the B+ tree storage engine.

#include <cstddef>
#include <cstdint>

namespace bptree {

// ---------------------------------------------------------------------------
// Page layout
// ---------------------------------------------------------------------------
constexpr size_t   PAGE_SIZE          = 4096;   ///< Bytes per disk page
constexpr size_t   DATA_SIZE          = 100;    ///< Fixed record payload size

// ---------------------------------------------------------------------------
// B+ tree fan-out (derived from page size)
// ---------------------------------------------------------------------------
/// Leaf: 16-byte header + N * (4-byte key + 100-byte data) <= PAGE_SIZE
constexpr int LEAF_MAX_KEYS     = 35;

/// Internal: 8-byte header + (N+1)*8-byte children + N*4-byte keys <= PAGE_SIZE
constexpr int INTERNAL_MAX_KEYS = 100;

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------
using page_id_t  = int64_t;
using key_t      = int;

constexpr page_id_t INVALID_PAGE_ID   = -1;
constexpr page_id_t HEADER_PAGE_SIZE  = PAGE_SIZE;  ///< byte-size of metadata page

// ---------------------------------------------------------------------------
// Metadata page layout (page 0)
//   [0..7]   root_offset      (int64_t, -1 if tree is empty)
//   [8..15]  next_page_off    (int64_t, next free offset)
//   [16..23] free_list_head   (int64_t, first free page, -1 if none)
// ---------------------------------------------------------------------------
constexpr size_t META_ROOT_OFFSET     = 0;
constexpr size_t META_NEXT_PAGE       = 8;
constexpr size_t META_FREE_LIST_HEAD  = 16;

// ---------------------------------------------------------------------------
// Free page: when a page is freed, byte 0..7 contains the offset of the
// next free page (linked list through freed pages).
// ---------------------------------------------------------------------------
constexpr size_t FREE_PAGE_NEXT_OFFSET = 0;

// ---------------------------------------------------------------------------
// Buffer pool default size
// ---------------------------------------------------------------------------
constexpr size_t DEFAULT_POOL_SIZE = 1024;  ///< 1024 frames = 4 MB

// ---------------------------------------------------------------------------
// B+ tree rebalancing thresholds
// ---------------------------------------------------------------------------
constexpr int LEAF_MIN_KEYS     = (LEAF_MAX_KEYS + 1) / 2;      ///< ceil(order/2)
constexpr int INTERNAL_MIN_KEYS = (INTERNAL_MAX_KEYS + 1) / 2;  ///< ceil(order/2)

// ---------------------------------------------------------------------------
// Default file name
// ---------------------------------------------------------------------------
constexpr const char* DEFAULT_INDEX_FILE = "bptree.idx";

}  // namespace bptree
