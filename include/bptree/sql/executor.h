/// @file executor.h
/// @brief SQL executor — runs AST nodes against B+ tree storage.

#pragma once

#include "bptree/sql/ast.h"
#include "bptree/sql/catalog.h"
#include "bptree/sql/row.h"
#include "bptree/bplus_tree.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// Result set
// ---------------------------------------------------------------------------

struct ResultSet {
    std::vector<std::string>    columns;  ///< column headers
    std::vector<Row>            rows;     ///< projected row data
    int                         affected = 0;  ///< for DML

    bool empty() const { return rows.empty(); }
};

// ---------------------------------------------------------------------------
// ExecutorError
// ---------------------------------------------------------------------------

struct ExecutorError : std::runtime_error {
    explicit ExecutorError(const std::string& msg) : std::runtime_error(msg) {}
};

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Executes a parsed SQL statement and returns a ResultSet.
/// Holds a Catalog and a pool of open B+ tree handles (one per table).
class Executor {
public:
    explicit Executor(const std::string& catalog_path = "catalog.dat",
                      const std::string& data_dir     = ".");

    /// Execute a parsed statement.
    ResultSet Execute(const Stmt& stmt);

    /// Convenience: parse and execute SQL text in one call.
    ResultSet ExecSQL(const std::string& sql);

private:
    // -- Statement handlers --
    ResultSet exec_create_table(const Stmt& s);
    ResultSet exec_drop_table  (const Stmt& s);
    ResultSet exec_insert      (const Stmt& s);
    ResultSet exec_select      (const Stmt& s);
    ResultSet exec_update      (const Stmt& s);
    ResultSet exec_delete      (const Stmt& s);

    // -- WHERE evaluation --
    bool eval_where(const Expr* where, const TableSchema& schema, const Row& row) const;
    Value eval_expr(const Expr& e, const TableSchema& schema, const Row& row) const;

    // -- Tree handle management --
    bptree::IntTree& open_tree(const std::string& table);
    std::string tree_path(const std::string& table) const;

    // -- Value coercion --
    static Value coerce(const Expr& e, ColType target);

    Catalog     catalog_;
    std::string data_dir_;
    std::unordered_map<std::string, std::unique_ptr<bptree::IntTree>> trees_;
};

} // namespace bptree::sql
