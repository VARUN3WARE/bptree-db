/// @file catalog.cpp
/// @brief Catalog implementation — schema load/save and table management.

#include "bptree/sql/catalog.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// TableSchema helpers
// ---------------------------------------------------------------------------

int TableSchema::col_index(const std::string& name) const {
    for (int i = 0; i < static_cast<int>(columns.size()); ++i) {
        if (columns[static_cast<size_t>(i)].name == name) return i;
    }
    return -1;
}

// ---------------------------------------------------------------------------
// Catalog ctor
// ---------------------------------------------------------------------------

Catalog::Catalog(const std::string& path) : path_(path) {
    load();  // silently no-ops if file doesn't exist yet
}

// ---------------------------------------------------------------------------
// Schema queries
// ---------------------------------------------------------------------------

bool Catalog::has_table(const std::string& name) const {
    return tables_.count(name) > 0;
}

const TableSchema& Catalog::get_table(const std::string& name) const {
    auto it = tables_.find(name);
    if (it == tables_.end())
        throw CatalogError("Table '" + name + "' does not exist");
    return it->second;
}

// ---------------------------------------------------------------------------
// Schema mutation
// ---------------------------------------------------------------------------

void Catalog::create_table(const TableSchema& schema) {
    if (has_table(schema.table_name))
        throw CatalogError("Table '" + schema.table_name + "' already exists");
    tables_[schema.table_name] = schema;
    save();
}

void Catalog::drop_table(const std::string& name) {
    if (!has_table(name))
        throw CatalogError("Table '" + name + "' does not exist");
    tables_.erase(name);
    save();
}

// ---------------------------------------------------------------------------
// Helpers: type <-> string
// ---------------------------------------------------------------------------

static std::string type_str(ColType t, int vlen) {
    switch (t) {
        case ColType::Int:     return "INT";
        case ColType::BigInt:  return "BIGINT";
        case ColType::Float:   return "FLOAT";
        case ColType::Varchar: return "VARCHAR(" + std::to_string(vlen) + ")";
    }
    return "INT";
}

static ColType parse_type(const std::string& s, int& vlen_out) {
    vlen_out = 0;
    if (s.rfind("VARCHAR", 0) == 0) {
        // VARCHAR(n)
        auto p = s.find('(');
        if (p != std::string::npos)
            vlen_out = std::stoi(s.substr(p + 1));
        return ColType::Varchar;
    }
    if (s == "BIGINT") return ColType::BigInt;
    if (s == "FLOAT")  return ColType::Float;
    return ColType::Int;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

void Catalog::save() const {
    std::ofstream f(path_);
    if (!f) throw CatalogError("Cannot write catalog file: " + path_);

    for (auto& [name, schema] : tables_) {
        f << "TABLE " << schema.table_name << "\n";
        for (auto& col : schema.columns) {
            f << "COL " << col.name
              << " "    << type_str(col.type, col.varchar_len)
              << " "    << (col.primary_key ? 1 : 0)
              << "\n";
        }
        f << "END\n";
    }
}

void Catalog::load() {
    std::ifstream f(path_);
    if (!f) return;  // first run — no catalog yet

    tables_.clear();
    std::string line;
    TableSchema cur;
    bool in_table = false;

    while (std::getline(f, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        std::string tag; ss >> tag;

        if (tag == "TABLE") {
            ss >> cur.table_name;
            cur.columns.clear();
            cur.pk_index = 0;
            in_table = true;
        } else if (tag == "COL" && in_table) {
            ColumnDef cd;
            std::string type_tok;
            int pk_flag = 0;
            ss >> cd.name >> type_tok >> pk_flag;
            cd.type        = parse_type(type_tok, cd.varchar_len);
            cd.primary_key = (pk_flag == 1);
            if (cd.primary_key)
                cur.pk_index = static_cast<int>(cur.columns.size());
            cur.columns.push_back(std::move(cd));
        } else if (tag == "END" && in_table) {
            tables_[cur.table_name] = cur;
            in_table = false;
        }
    }
}

} // namespace bptree::sql
