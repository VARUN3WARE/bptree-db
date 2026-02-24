/// @file parser.h
/// @brief Recursive-descent parser. Token stream in, AST out.

#pragma once

#include "bptree/sql/ast.h"
#include "bptree/sql/token.h"

#include <stdexcept>
#include <string>
#include <vector>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// ParseError
// ---------------------------------------------------------------------------

/// Thrown when the token stream doesn't match the grammar.
struct ParseError : std::runtime_error {
    explicit ParseError(const std::string& msg) : std::runtime_error(msg) {}
};

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

class Parser {
public:
    /// Parse a single SQL statement.
    /// @throws ParseError on syntax errors.
    static StmtPtr Parse(const std::vector<Token>& tokens);

private:
    explicit Parser(const std::vector<Token>& tokens) : tokens_(tokens) {}

    StmtPtr parse_stmt();

    // -- DDL --
    StmtPtr parse_create_table();
    StmtPtr parse_drop_table();
    ColumnDef parse_column_def();

    // -- DML --
    StmtPtr parse_insert();
    StmtPtr parse_select();
    StmtPtr parse_update();
    StmtPtr parse_delete();

    // -- Expressions --
    ExprPtr parse_expr();      ///< entry point (handles AND/OR)
    ExprPtr parse_comparison();
    ExprPtr parse_primary();

    // -- Helpers --
    const Token& peek(int offset = 0) const;
    const Token& advance();
    bool  check(TokenKind k) const;
    bool  match(TokenKind k);
    const Token& expect(TokenKind k, const std::string& msg);

    std::string parse_ident(const std::string& ctx);

    const std::vector<Token>& tokens_;
    size_t pos_ = 0;
};

} // namespace bptree::sql
