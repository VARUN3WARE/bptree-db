/// @file ast.h
/// @brief AST nodes for the SQL subset.
///        Trees within trees — it's turtles all the way down. :)

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <optional>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------
struct Expr;
struct Stmt;

using ExprPtr = std::unique_ptr<Expr>;
using StmtPtr = std::unique_ptr<Stmt>;

// ===========================================================================
// Expressions
// ===========================================================================

enum class ExprKind {
    IntLit,     ///< integer literal: 42
    FloatLit,   ///< float literal: 3.14
    StrLit,     ///< string literal: 'hello'
    ColRef,     ///< column reference: age, users.age
    BinaryOp,   ///< a op b
    UnaryNot,   ///< NOT expr
    Null,       ///< NULL
};

enum class BinOp { Eq, NEq, Lt, LEq, Gt, GEq, And, Or, Plus, Minus };

struct Expr {
    ExprKind kind;

    // -- Literal values (only one is valid at a time) --
    int64_t     int_val  = 0;
    double      flt_val  = 0.0;
    std::string str_val;        ///< string literal text or column name

    // -- Binary / unary op --
    BinOp    op{};
    ExprPtr  left;
    ExprPtr  right;

    // Convenience factories.
    static ExprPtr MakeInt(int64_t v);
    static ExprPtr MakeFloat(double v);
    static ExprPtr MakeStr(const std::string& s);
    static ExprPtr MakeCol(const std::string& col);
    static ExprPtr MakeBin(BinOp op, ExprPtr l, ExprPtr r);
    static ExprPtr MakeNull();
};

// ===========================================================================
// Column definition (used by CREATE TABLE)
// ===========================================================================

enum class ColType { Int, BigInt, Varchar, Float };

struct ColumnDef {
    std::string name;
    ColType     type        = ColType::Int;
    int         varchar_len = 0;    ///< only valid when type == Varchar
    bool        primary_key = false;
};

// ===========================================================================
// Statements
// ===========================================================================

enum class StmtKind {
    CreateTable, DropTable,
    Insert, Select, Update, Delete,
};

// -- Assignment in UPDATE SET --
struct Assignment {
    std::string col;
    ExprPtr     value;
};

struct Stmt {
    StmtKind kind;

    // -- CREATE TABLE --
    std::string             table_name;
    std::vector<ColumnDef>  columns;        // also used by INSERT column list

    // -- INSERT --
    std::vector<std::string>             insert_cols;   ///< explicit column list (optional)
    std::vector<std::vector<ExprPtr>>    insert_rows;   ///< VALUES (...), (...)

    // -- SELECT --
    std::vector<std::string> select_cols;   ///< column names, or {"*"}

    // -- UPDATE --
    std::vector<Assignment>  assignments;

    // -- WHERE (shared by SELECT / UPDATE / DELETE) --
    ExprPtr where;

    Stmt() = default;
    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;
};

} // namespace bptree::sql
