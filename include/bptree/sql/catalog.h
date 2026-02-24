/// @file catalog.h
/// @brief Schema manager: knows which tables exist and what columns they have.

#pragma once

#include "bptree/sql/ast.h"   // ColType, ColumnDef

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// TableSchema
// ---------------------------------------------------------------------------

struct TableSchema {
    std::string            table_name;
    std::vector<ColumnDef> columns;
    int                    pk_index = 0;  ///< index of the PRIMARY KEY column

    /// Returns -1 if not found.
    int col_index(const std::string& name) const;

    /// Short-hand to the PK column def.
    const ColumnDef& pk_col() const { return columns[static_cast<size_t>(pk_index)]; }
};

// ---------------------------------------------------------------------------
// CatalogError
// ---------------------------------------------------------------------------

struct CatalogError : std::runtime_error {
    explicit CatalogError(const std::string& msg) : std::runtime_error(msg) {}
};

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/// Manages in-memory schema metadata, persisted to a plain text file.
/// Format (one line per column):
///   TABLE <name>
///   COL   <name> <type> <varchar_len> <pk:0|1>
///   END
class Catalog {
public:
    explicit Catalog(const std::string& path = "catalog.dat");

    // -- Schema queries --
    bool               has_table(const std::string& name) const;
    const TableSchema& get_table(const std::string& name) const;

    // -- Schema mutation --
    void create_table(const TableSchema& schema);
    void drop_table  (const std::string& name);

    // -- Persistence --
    void save() const;
    void load();

    const std::string& path() const { return path_; }

private:
    std::string                                  path_;
    std::unordered_map<std::string, TableSchema> tables_;
};

} // namespace bptree::sql
