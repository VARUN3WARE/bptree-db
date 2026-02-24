/// @file executor.cpp
/// @brief SQL executor implementation.
///        This is the part where all the other pieces meet. No pressure. :)

#include "bptree/sql/executor.h"
#include "bptree/sql/lexer.h"
#include "bptree/sql/parser.h"
#include "bptree/sql/row.h"

#include <algorithm>
#include <climits>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <variant>

namespace fs = std::filesystem;

namespace bptree::sql {

// ---------------------------------------------------------------------------
// Ctor
// ---------------------------------------------------------------------------

Executor::Executor(const std::string& catalog_path, const std::string& data_dir)
    : catalog_(catalog_path), data_dir_(data_dir) {}

// ---------------------------------------------------------------------------
// Public Execute
// ---------------------------------------------------------------------------

ResultSet Executor::Execute(const Stmt& stmt) {
    switch (stmt.kind) {
        case StmtKind::CreateTable: return exec_create_table(stmt);
        case StmtKind::DropTable:   return exec_drop_table(stmt);
        case StmtKind::Insert:      return exec_insert(stmt);
        case StmtKind::Select:      return exec_select(stmt);
        case StmtKind::Update:      return exec_update(stmt);
        case StmtKind::Delete:      return exec_delete(stmt);
    }
    throw ExecutorError("Unknown statement kind");
}

ResultSet Executor::ExecSQL(const std::string& sql) {
    auto tokens = Lexer::Tokenise(sql);
    auto stmt   = Parser::Parse(tokens);
    return Execute(*stmt);
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

ResultSet Executor::exec_create_table(const Stmt& s) {
    TableSchema schema;
    schema.table_name = s.table_name;
    schema.columns    = s.columns;
    schema.pk_index   = 0;

    bool found_pk = false;
    for (int i = 0; i < static_cast<int>(schema.columns.size()); ++i) {
        if (schema.columns[static_cast<size_t>(i)].primary_key) {
            if (schema.columns[static_cast<size_t>(i)].type != ColType::Int)
                throw ExecutorError("Primary key must be INT (Phase 3 limitation)");
            schema.pk_index = i;
            found_pk = true;
            break;
        }
    }
    if (!found_pk)
        throw ExecutorError("Table '" + s.table_name + "' must have a PRIMARY KEY column");

    size_t row_bytes = RowSerializer::RowBytes(schema);
    if (row_bytes > DATA_SIZE)
        throw ExecutorError("Schema too wide: " + std::to_string(row_bytes) +
                            " bytes > DATA_SIZE=" + std::to_string(DATA_SIZE));

    catalog_.create_table(schema);

    // Create (or open) the backing B+ tree file immediately.
    open_tree(s.table_name);

    ResultSet rs;
    rs.affected = 0;
    rs.columns  = {"result"};
    rs.rows     = {{std::string("Table '" + s.table_name + "' created")}};
    return rs;
}

ResultSet Executor::exec_drop_table(const Stmt& s) {
    // Close and remove in-memory handle.
    trees_.erase(s.table_name);

    // Delete the index file.
    std::string path = tree_path(s.table_name);
    fs::remove(path);
    fs::remove(path + ".wal");

    catalog_.drop_table(s.table_name);

    ResultSet rs;
    rs.columns = {"result"};
    rs.rows    = {{std::string("Table '" + s.table_name + "' dropped")}};
    return rs;
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

ResultSet Executor::exec_insert(const Stmt& s) {
    const TableSchema& schema = catalog_.get_table(s.table_name);
    bptree::IntTree&   tree   = open_tree(s.table_name);

    int inserted = 0;
    for (auto& expr_row : s.insert_rows) {
        // Build Row in schema column order
        Row row(schema.columns.size());

        if (!s.insert_cols.empty()) {
            // Explicit column list: match by name.
            if (expr_row.size() != s.insert_cols.size())
                throw ExecutorError("Column and value count mismatch");
            for (size_t i = 0; i < s.insert_cols.size(); ++i) {
                int ci = schema.col_index(s.insert_cols[i]);
                if (ci < 0) throw ExecutorError("Column '" + s.insert_cols[i] + "' not found");
                row[static_cast<size_t>(ci)] = coerce(*expr_row[i], schema.columns[static_cast<size_t>(ci)].type);
            }
        } else {
            // Positional.
            if (expr_row.size() != schema.columns.size())
                throw ExecutorError("Value count doesn't match column count");
            for (size_t i = 0; i < schema.columns.size(); ++i)
                row[i] = coerce(*expr_row[i], schema.columns[i].type);
        }

        char buf[DATA_SIZE]{};
        RowSerializer::Encode(schema, row, buf);

        int32_t pk = RowSerializer::PkInt(schema, row);
        auto status = tree.Insert(pk, buf);
        if (!status.ok())
            throw ExecutorError("Insert failed: " + status.ToString());
        ++inserted;
    }

    ResultSet rs;
    rs.affected = inserted;
    rs.columns  = {"rows_inserted"};
    rs.rows     = {{static_cast<int32_t>(inserted)}};
    return rs;
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

ResultSet Executor::exec_select(const Stmt& s) {
    const TableSchema& schema = catalog_.get_table(s.table_name);
    bptree::IntTree&   tree   = open_tree(s.table_name);

    // Full scan via range query.
    std::vector<std::pair<int, std::string>> raw;
    tree.RangeQuery(INT_MIN, INT_MAX, raw);

    // Determine projection columns.
    bool star = (s.select_cols.size() == 1 && s.select_cols[0] == "*");
    std::vector<int> proj_indices;
    std::vector<std::string> headers;
    if (star) {
        for (int i = 0; i < static_cast<int>(schema.columns.size()); ++i)
            proj_indices.push_back(i);
        for (auto& cd : schema.columns) headers.push_back(cd.name);
    } else {
        for (auto& col : s.select_cols) {
            int ci = schema.col_index(col);
            if (ci < 0) throw ExecutorError("Unknown column: " + col);
            proj_indices.push_back(ci);
            headers.push_back(col);
        }
    }

    ResultSet rs;
    rs.columns = headers;
    for (auto& [key, val_str] : raw) {
        Row full = RowSerializer::Decode(schema, val_str.data());

        // Apply WHERE filter.
        if (s.where && !eval_where(s.where.get(), schema, full)) continue;

        // Project.
        Row projected;
        projected.reserve(proj_indices.size());
        for (int ci : proj_indices) projected.push_back(full[static_cast<size_t>(ci)]);
        rs.rows.push_back(std::move(projected));
    }

    return rs;
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

ResultSet Executor::exec_update(const Stmt& s) {
    const TableSchema& schema = catalog_.get_table(s.table_name);
    bptree::IntTree&   tree   = open_tree(s.table_name);

    std::vector<std::pair<int, std::string>> raw;
    tree.RangeQuery(INT_MIN, INT_MAX, raw);

    int updated = 0;
    for (auto& [key, val_str] : raw) {
        Row full = RowSerializer::Decode(schema, val_str.data());

        if (s.where && !eval_where(s.where.get(), schema, full)) continue;

        // Apply assignments.
        for (auto& asgn : s.assignments) {
            int ci = schema.col_index(asgn.col);
            if (ci < 0) throw ExecutorError("Column '" + asgn.col + "' not found");
            full[static_cast<size_t>(ci)] = coerce(*asgn.value, schema.columns[static_cast<size_t>(ci)].type);
        }

        char buf[DATA_SIZE]{};
        RowSerializer::Encode(schema, full, buf);
        int32_t pk = RowSerializer::PkInt(schema, full);
        tree.Insert(pk, buf);  // upsert
        ++updated;
    }

    ResultSet rs;
    rs.affected = updated;
    rs.columns  = {"rows_updated"};
    rs.rows     = {{static_cast<int32_t>(updated)}};
    return rs;
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

ResultSet Executor::exec_delete(const Stmt& s) {
    const TableSchema& schema = catalog_.get_table(s.table_name);
    bptree::IntTree&   tree   = open_tree(s.table_name);

    std::vector<std::pair<int, std::string>> raw;
    tree.RangeQuery(INT_MIN, INT_MAX, raw);

    int deleted = 0;
    for (auto& [key, val_str] : raw) {
        Row full = RowSerializer::Decode(schema, val_str.data());
        if (s.where && !eval_where(s.where.get(), schema, full)) continue;

        int32_t pk = RowSerializer::PkInt(schema, full);
        tree.Delete(pk);
        ++deleted;
    }

    ResultSet rs;
    rs.affected = deleted;
    rs.columns  = {"rows_deleted"};
    rs.rows     = {{static_cast<int32_t>(deleted)}};
    return rs;
}

// ---------------------------------------------------------------------------
// WHERE evaluation
// ---------------------------------------------------------------------------

Value Executor::eval_expr(const Expr& e, const TableSchema& schema, const Row& row) const {
    switch (e.kind) {
        case ExprKind::IntLit:   return static_cast<int32_t>(e.int_val);
        case ExprKind::FloatLit: return e.flt_val;
        case ExprKind::StrLit:   return e.str_val;
        case ExprKind::Null:     return std::string("NULL");
        case ExprKind::ColRef: {
            int ci = schema.col_index(e.str_val);
            if (ci < 0) throw ExecutorError("Unknown column: " + e.str_val);
            return row[static_cast<size_t>(ci)];
        }
        case ExprKind::BinaryOp:
        case ExprKind::UnaryNot:
            // Handled in eval_where
            return static_cast<int32_t>(0);
    }
    return static_cast<int32_t>(0);
}

bool Executor::eval_where(const Expr* where, const TableSchema& schema, const Row& row) const {
    if (!where) return true;

    if (where->kind == ExprKind::BinaryOp) {
        if (where->op == BinOp::And)
            return eval_where(where->left.get(),  schema, row) &&
                   eval_where(where->right.get(), schema, row);
        if (where->op == BinOp::Or)
            return eval_where(where->left.get(),  schema, row) ||
                   eval_where(where->right.get(), schema, row);

        Value lv = eval_expr(*where->left,  schema, row);
        Value rv = eval_expr(*where->right, schema, row);

        // Comparison: try numeric first, then string
        auto cmp = [&]() -> int {
            // Both numeric?
            auto to_double = [](const Value& v) -> double {
                return std::visit([](auto&& a) -> double {
                    using T = std::decay_t<decltype(a)>;
                    if constexpr (std::is_same_v<T, std::string>) return 0.0;
                    else return static_cast<double>(a);
                }, v);
            };
            bool lstr = std::holds_alternative<std::string>(lv);
            bool rstr = std::holds_alternative<std::string>(rv);
            if (lstr && rstr) {
                auto& ls = std::get<std::string>(lv);
                auto& rs = std::get<std::string>(rv);
                return ls < rs ? -1 : (ls == rs ? 0 : 1);
            }
            double ld = to_double(lv), rd = to_double(rv);
            return ld < rd ? -1 : (ld == rd ? 0 : 1);
        };

        int c = cmp();
        switch (where->op) {
            case BinOp::Eq:  return c == 0;
            case BinOp::NEq: return c != 0;
            case BinOp::Lt:  return c <  0;
            case BinOp::LEq: return c <= 0;
            case BinOp::Gt:  return c >  0;
            case BinOp::GEq: return c >= 0;
            default: return true;
        }
    }

    if (where->kind == ExprKind::UnaryNot)
        return !eval_where(where->left.get(), schema, row);

    // Scalar — treat non-zero / non-empty as truthy
    Value v = eval_expr(*where, schema, row);
    return std::visit([](auto&& a) -> bool {
        using T = std::decay_t<decltype(a)>;
        if constexpr (std::is_same_v<T, std::string>) return !a.empty();
        else return a != T{};
    }, v);
}

// ---------------------------------------------------------------------------
// Tree handle management
// ---------------------------------------------------------------------------

bptree::IntTree& Executor::open_tree(const std::string& table) {
    auto it = trees_.find(table);
    if (it != trees_.end()) return *it->second;

    auto tree = std::make_unique<bptree::IntTree>(tree_path(table));
    auto& ref = *tree;
    trees_[table] = std::move(tree);
    return ref;
}

std::string Executor::tree_path(const std::string& table) const {
    return data_dir_ + "/" + table + ".idx";
}

// ---------------------------------------------------------------------------
// Value coercion
// ---------------------------------------------------------------------------

Value Executor::coerce(const Expr& e, ColType target) {
    switch (target) {
        case ColType::Int:
            if (e.kind == ExprKind::IntLit)   return static_cast<int32_t>(e.int_val);
            if (e.kind == ExprKind::FloatLit)  return static_cast<int32_t>(e.flt_val);
            if (e.kind == ExprKind::StrLit)    return static_cast<int32_t>(std::stoi(e.str_val));
            throw ExecutorError("Cannot coerce to INT");

        case ColType::BigInt:
            if (e.kind == ExprKind::IntLit)   return static_cast<int64_t>(e.int_val);
            if (e.kind == ExprKind::FloatLit)  return static_cast<int64_t>(e.flt_val);
            throw ExecutorError("Cannot coerce to BIGINT");

        case ColType::Float:
            if (e.kind == ExprKind::IntLit)   return static_cast<double>(e.int_val);
            if (e.kind == ExprKind::FloatLit)  return e.flt_val;
            throw ExecutorError("Cannot coerce to FLOAT");

        case ColType::Varchar:
            if (e.kind == ExprKind::StrLit)   return e.str_val;
            if (e.kind == ExprKind::IntLit)   return std::to_string(e.int_val);
            throw ExecutorError("Cannot coerce to VARCHAR");
    }
    throw ExecutorError("Unknown column type");
}

} // namespace bptree::sql
