/// @file row.cpp
/// @brief Row serializer implementation.

#include "bptree/sql/row.h"

#include <cstring>
#include <sstream>
#include <stdexcept>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// value_to_string
// ---------------------------------------------------------------------------

std::string value_to_string(const Value& v) {
    return std::visit([](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::string>) return arg;
        else return std::to_string(arg);
    }, v);
}

// ---------------------------------------------------------------------------
// RowSerializer
// ---------------------------------------------------------------------------

size_t RowSerializer::RowBytes(const TableSchema& schema) {
    size_t total = 0;
    for (auto& cd : schema.columns) total += col_storage_size(cd);
    return total;
}

void RowSerializer::Encode(const TableSchema& schema, const Row& row, char* buf) {
    if (row.size() != schema.columns.size())
        throw std::runtime_error("Row column count mismatch");
    if (RowBytes(schema) > DATA_SIZE)
        throw std::runtime_error("Row too wide for DATA_SIZE=" + std::to_string(DATA_SIZE));

    std::memset(buf, 0, DATA_SIZE);
    size_t off = 0;
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const ColumnDef& cd = schema.columns[i];
        switch (cd.type) {
            case ColType::Int: {
                int32_t v = std::get<int32_t>(row[i]);
                std::memcpy(buf + off, &v, 4);
                off += 4;
                break;
            }
            case ColType::BigInt: {
                int64_t v = std::get<int64_t>(row[i]);
                std::memcpy(buf + off, &v, 8);
                off += 8;
                break;
            }
            case ColType::Float: {
                double v = std::get<double>(row[i]);
                std::memcpy(buf + off, &v, 8);
                off += 8;
                break;
            }
            case ColType::Varchar: {
                const std::string& s = std::get<std::string>(row[i]);
                size_t n = std::min(s.size(), kVarcharStorageSize - 1);
                std::memcpy(buf + off, s.data(), n);
                off += kVarcharStorageSize;
                break;
            }
        }
    }
}

Row RowSerializer::Decode(const TableSchema& schema, const char* buf) {
    Row row;
    row.reserve(schema.columns.size());
    size_t off = 0;
    for (auto& cd : schema.columns) {
        switch (cd.type) {
            case ColType::Int: {
                int32_t v;
                std::memcpy(&v, buf + off, 4);
                row.push_back(v);
                off += 4;
                break;
            }
            case ColType::BigInt: {
                int64_t v;
                std::memcpy(&v, buf + off, 8);
                row.push_back(v);
                off += 8;
                break;
            }
            case ColType::Float: {
                double v;
                std::memcpy(&v, buf + off, 8);
                row.push_back(v);
                off += 8;
                break;
            }
            case ColType::Varchar: {
                std::string s(buf + off, ::strnlen(buf + off, kVarcharStorageSize));
                row.push_back(std::move(s));
                off += kVarcharStorageSize;
                break;
            }
        }
    }
    return row;
}

int32_t RowSerializer::PkInt(const TableSchema& schema, const Row& row) {
    return std::get<int32_t>(row[static_cast<size_t>(schema.pk_index)]);
}

} // namespace bptree::sql
