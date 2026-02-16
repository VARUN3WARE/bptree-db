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
//   [0..7]   root_offset   (int64_t, -1 if tree is empty)
//   [8..15]  next_page_off (int64_t, next free offset)
// ---------------------------------------------------------------------------
constexpr size_t META_ROOT_OFFSET     = 0;
constexpr size_t META_NEXT_PAGE       = 8;

// ---------------------------------------------------------------------------
// Default file name
// ---------------------------------------------------------------------------
constexpr const char* DEFAULT_INDEX_FILE = "bptree.idx";

}  // namespace bptree
