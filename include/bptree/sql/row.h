/// @file row.h
/// @brief Value variant, Row type, and row serializer.
///        Packing column values into 256 bytes. It's like Tetris, but louder.

#pragma once

#include "bptree/sql/catalog.h"
#include "bptree/config.h"

#include <cstring>
#include <string>
#include <variant>
#include <vector>
#include <stdexcept>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// Value — a single column value
// ---------------------------------------------------------------------------

using Value = std::variant<int32_t, int64_t, double, std::string>;

/// Tagged column index, value pair.
struct NamedValue {
    std::string name;
    Value       val;
};

/// A full row: values for every column, in schema order.
using Row = std::vector<Value>;

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

inline int32_t     val_int  (const Value& v) { return std::get<int32_t>(v); }
inline int64_t     val_bigint(const Value& v){ return std::get<int64_t>(v); }
inline double      val_float (const Value& v){ return std::get<double>(v); }
inline std::string val_str  (const Value& v) { return std::get<std::string>(v); }

std::string value_to_string(const Value& v);

// ---------------------------------------------------------------------------
// Column byte sizes (fixed per type, stored inside the 256-byte payload)
// ---------------------------------------------------------------------------
// Layout inside DATA_SIZE bytes:
//   For each column in schema order:
//     INT      => 4 bytes
//     BIGINT   => 8 bytes
//     FLOAT    => 8 bytes
//     VARCHAR  => 64 bytes (null-padded, truncated if longer)
// ---------------------------------------------------------------------------

constexpr size_t kVarcharStorageSize = 64;

inline size_t col_storage_size(const ColumnDef& cd) {
    switch (cd.type) {
        case ColType::Int:     return 4;
        case ColType::BigInt:  return 8;
        case ColType::Float:   return 8;
        case ColType::Varchar: return kVarcharStorageSize;
    }
    return 4;
}

// ---------------------------------------------------------------------------
// RowSerializer
// ---------------------------------------------------------------------------

class RowSerializer {
public:
    /// Encode a Row into a fixed DATA_SIZE buffer.
    /// Callers own the buffer lifetime.
    static void Encode(const TableSchema& schema, const Row& row, char* buf);

    /// Decode a DATA_SIZE buffer back into a Row.
    static Row  Decode(const TableSchema& schema, const char* buf);

    /// Total bytes needed by schema — must be <= DATA_SIZE.
    static size_t RowBytes(const TableSchema& schema);

    /// Extract just the primary-key int32 value from a row.
    static int32_t PkInt(const TableSchema& schema, const Row& row);
};

} // namespace bptree::sql
