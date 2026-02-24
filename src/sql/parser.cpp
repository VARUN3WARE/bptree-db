/// @file parser.cpp
/// @brief Recursive-descent parser for the SQL subset.
///        Grammar (simplified):
///          stmt  := create | drop | insert | select | update | delete
///          expr  := comparison (AND|OR comparison)*
///          comparison := primary (op primary)?

#include "bptree/sql/parser.h"

#include <sstream>
#include <stdexcept>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// Public entry-point
// ---------------------------------------------------------------------------

StmtPtr Parser::Parse(const std::vector<Token>& tokens) {
    Parser p(tokens);
    return p.parse_stmt();
}

// ---------------------------------------------------------------------------
// Statement dispatch
// ---------------------------------------------------------------------------

StmtPtr Parser::parse_stmt() {
    const Token& tok = peek();

    if (tok.is(TokenKind::KwCreate)) return parse_create_table();
    if (tok.is(TokenKind::KwDrop))   return parse_drop_table();
    if (tok.is(TokenKind::KwInsert)) return parse_insert();
    if (tok.is(TokenKind::KwSelect)) return parse_select();
    if (tok.is(TokenKind::KwUpdate)) return parse_update();
    if (tok.is(TokenKind::KwDelete)) return parse_delete();

    throw ParseError("Expected a SQL statement keyword, got: '" + tok.text + "'");
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

StmtPtr Parser::parse_create_table() {
    expect(TokenKind::KwCreate, "CREATE");
    expect(TokenKind::KwTable,  "TABLE");

    auto stmt = std::make_unique<Stmt>();
    stmt->kind       = StmtKind::CreateTable;
    stmt->table_name = parse_ident("table name");

    expect(TokenKind::LParen, "(");
    do {
        stmt->columns.push_back(parse_column_def());
    } while (match(TokenKind::Comma));
    expect(TokenKind::RParen, ")");
    match(TokenKind::Semi);

    return stmt;
}

ColumnDef Parser::parse_column_def() {
    ColumnDef cd;
    cd.name = parse_ident("column name");

    if      (match(TokenKind::KwInt))    { cd.type = ColType::Int; }
    else if (match(TokenKind::KwBigInt)) { cd.type = ColType::BigInt; }
    else if (match(TokenKind::KwFloat))  { cd.type = ColType::Float; }
    else if (match(TokenKind::KwVarchar)) {
        cd.type = ColType::Varchar;
        expect(TokenKind::LParen, "(");
        cd.varchar_len = static_cast<int>(std::stoll(expect(TokenKind::IntLit, "length").text));
        expect(TokenKind::RParen, ")");
    } else {
        throw ParseError("Expected column type, got: '" + peek().text + "'");
    }

    // Optional PRIMARY KEY
    if (match(TokenKind::KwPrimary)) {
        expect(TokenKind::KwKey, "KEY");
        cd.primary_key = true;
    }

    return cd;
}

StmtPtr Parser::parse_drop_table() {
    expect(TokenKind::KwDrop,  "DROP");
    expect(TokenKind::KwTable, "TABLE");

    auto stmt = std::make_unique<Stmt>();
    stmt->kind       = StmtKind::DropTable;
    stmt->table_name = parse_ident("table name");
    match(TokenKind::Semi);

    return stmt;
}

// ---------------------------------------------------------------------------
// DML — INSERT
// ---------------------------------------------------------------------------

StmtPtr Parser::parse_insert() {
    expect(TokenKind::KwInsert, "INSERT");
    expect(TokenKind::KwInto,   "INTO");

    auto stmt = std::make_unique<Stmt>();
    stmt->kind       = StmtKind::Insert;
    stmt->table_name = parse_ident("table name");

    // Optional explicit column list
    if (match(TokenKind::LParen)) {
        do {
            stmt->insert_cols.push_back(parse_ident("column name"));
        } while (match(TokenKind::Comma));
        expect(TokenKind::RParen, ")");
    }

    expect(TokenKind::KwValues, "VALUES");

    // One or more rows
    do {
        expect(TokenKind::LParen, "(");
        std::vector<ExprPtr> row;
        do {
            row.push_back(parse_primary());
        } while (match(TokenKind::Comma));
        expect(TokenKind::RParen, ")");
        stmt->insert_rows.push_back(std::move(row));
    } while (match(TokenKind::Comma));

    match(TokenKind::Semi);
    return stmt;
}

// ---------------------------------------------------------------------------
// DML — SELECT
// ---------------------------------------------------------------------------

StmtPtr Parser::parse_select() {
    expect(TokenKind::KwSelect, "SELECT");

    auto stmt = std::make_unique<Stmt>();
    stmt->kind = StmtKind::Select;

    if (match(TokenKind::Star)) {
        stmt->select_cols.push_back("*");
    } else {
        do {
            stmt->select_cols.push_back(parse_ident("column name"));
        } while (match(TokenKind::Comma));
    }

    expect(TokenKind::KwFrom, "FROM");
    stmt->table_name = parse_ident("table name");

    if (match(TokenKind::KwWhere)) {
        stmt->where = parse_expr();
    }

    match(TokenKind::Semi);
    return stmt;
}

// ---------------------------------------------------------------------------
// DML — UPDATE
// ---------------------------------------------------------------------------

StmtPtr Parser::parse_update() {
    expect(TokenKind::KwUpdate, "UPDATE");

    auto stmt = std::make_unique<Stmt>();
    stmt->kind       = StmtKind::Update;
    stmt->table_name = parse_ident("table name");

    expect(TokenKind::KwSet, "SET");

    do {
        Assignment asgn;
        asgn.col   = parse_ident("column name");
        expect(TokenKind::Eq,  "=");
        asgn.value = parse_primary();
        stmt->assignments.push_back(std::move(asgn));
    } while (match(TokenKind::Comma));

    if (match(TokenKind::KwWhere)) {
        stmt->where = parse_expr();
    }

    match(TokenKind::Semi);
    return stmt;
}

// ---------------------------------------------------------------------------
// DML — DELETE
// ---------------------------------------------------------------------------

StmtPtr Parser::parse_delete() {
    expect(TokenKind::KwDelete, "DELETE");
    expect(TokenKind::KwFrom,   "FROM");

    auto stmt = std::make_unique<Stmt>();
    stmt->kind       = StmtKind::Delete;
    stmt->table_name = parse_ident("table name");

    if (match(TokenKind::KwWhere)) {
        stmt->where = parse_expr();
    }

    match(TokenKind::Semi);
    return stmt;
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

ExprPtr Parser::parse_expr() {
    auto lhs = parse_comparison();

    while (check(TokenKind::KwAnd) || check(TokenKind::KwOr)) {
        BinOp op = peek().is(TokenKind::KwAnd) ? BinOp::And : BinOp::Or;
        advance();
        auto rhs = parse_comparison();
        lhs = Expr::MakeBin(op, std::move(lhs), std::move(rhs));
    }

    return lhs;
}

ExprPtr Parser::parse_comparison() {
    auto lhs = parse_primary();

    BinOp op{};
    bool  has_op = true;
    if      (match(TokenKind::Eq))   op = BinOp::Eq;
    else if (match(TokenKind::NEq))  op = BinOp::NEq;
    else if (match(TokenKind::Lt))   op = BinOp::Lt;
    else if (match(TokenKind::LEq))  op = BinOp::LEq;
    else if (match(TokenKind::Gt))   op = BinOp::Gt;
    else if (match(TokenKind::GEq))  op = BinOp::GEq;
    else has_op = false;

    if (has_op) {
        auto rhs = parse_primary();
        return Expr::MakeBin(op, std::move(lhs), std::move(rhs));
    }

    return lhs;
}

ExprPtr Parser::parse_primary() {
    const Token& t = peek();

    if (t.is(TokenKind::IntLit)) {
        advance();
        return Expr::MakeInt(std::stoll(t.text));
    }
    if (t.is(TokenKind::FloatLit)) {
        advance();
        return Expr::MakeFloat(std::stod(t.text));
    }
    if (t.is(TokenKind::StrLit)) {
        advance();
        return Expr::MakeStr(t.text);
    }
    if (t.is(TokenKind::KwNull)) {
        advance();
        return Expr::MakeNull();
    }
    if (t.is(TokenKind::Minus)) {  // negative literal
        advance();
        const Token& num = peek();
        if (num.is(TokenKind::IntLit)) {
            advance();
            return Expr::MakeInt(-std::stoll(num.text));
        }
        if (num.is(TokenKind::FloatLit)) {
            advance();
            return Expr::MakeFloat(-std::stod(num.text));
        }
        throw ParseError("Expected numeric literal after '-'");
    }
    if (t.is(TokenKind::LParen)) {
        advance();
        auto e = parse_expr();
        expect(TokenKind::RParen, ")");
        return e;
    }
    if (t.is(TokenKind::Ident)) {
        advance();
        return Expr::MakeCol(t.text);
    }

    throw ParseError("Unexpected token in expression: '" + t.text + "'");
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

const Token& Parser::peek(int offset) const {
    size_t idx = pos_ + static_cast<size_t>(offset);
    if (idx >= tokens_.size()) return tokens_.back();  // Eof
    return tokens_[idx];
}

const Token& Parser::advance() {
    const Token& t = tokens_[pos_];
    if (pos_ + 1 < tokens_.size()) ++pos_;
    return t;
}

bool Parser::check(TokenKind k) const {
    return peek().is(k);
}

bool Parser::match(TokenKind k) {
    if (check(k)) { advance(); return true; }
    return false;
}

const Token& Parser::expect(TokenKind k, const std::string& what) {
    if (!check(k)) {
        throw ParseError("Expected '" + what + "', got '" + peek().text + "'");
    }
    return advance();
}

std::string Parser::parse_ident(const std::string& ctx) {
    if (!check(TokenKind::Ident)) {
        // Keywords can serve as identifiers in some positions (e.g. table named "key")
        if (peek().isKeyword()) return advance().text;
        throw ParseError("Expected identifier (" + ctx + "), got '" + peek().text + "'");
    }
    return advance().text;
}

} // namespace bptree::sql
