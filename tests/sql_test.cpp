/// @file sql_test.cpp
/// @brief Unit tests for the SQL layer: Lexer, Parser, and Executor.
///        Three suites for the price of one. Bargain :)

#include <gtest/gtest.h>

#include "bptree/sql/lexer.h"
#include "bptree/sql/parser.h"
#include "bptree/sql/executor.h"
#include "bptree/sql/row.h"

#include <cstdio>
#include <filesystem>

namespace fs = std::filesystem;
using namespace bptree::sql;

// ============================================================================
// LexerTest
// ============================================================================

class LexerTest : public ::testing::Test {};

TEST_F(LexerTest, TokenisesKeywords) {
    auto toks = Lexer::Tokenise("SELECT * FROM users WHERE id = 1;");
    EXPECT_EQ(toks[0].kind, TokenKind::KwSelect);
    EXPECT_EQ(toks[1].kind, TokenKind::Star);
    EXPECT_EQ(toks[2].kind, TokenKind::KwFrom);
    EXPECT_EQ(toks[3].kind, TokenKind::Ident);
    EXPECT_EQ(toks[3].text, "users");
    EXPECT_EQ(toks[4].kind, TokenKind::KwWhere);
    EXPECT_EQ(toks[5].kind, TokenKind::Ident);
    EXPECT_EQ(toks[6].kind, TokenKind::Eq);
    EXPECT_EQ(toks[7].kind, TokenKind::IntLit);
    EXPECT_EQ(toks[7].text, "1");
}

TEST_F(LexerTest, TokenisesStringLiteral) {
    auto toks = Lexer::Tokenise("INSERT INTO t VALUES ('hello world');");
    bool found = false;
    for (auto& t : toks) {
        if (t.kind == TokenKind::StrLit && t.text == "hello world") { found = true; break; }
    }
    EXPECT_TRUE(found);
}

TEST_F(LexerTest, TokenisesFloatLiteral) {
    auto toks = Lexer::Tokenise("3.14");
    EXPECT_EQ(toks[0].kind, TokenKind::FloatLit);
    EXPECT_EQ(toks[0].text, "3.14");
}

TEST_F(LexerTest, SkipsSingleLineComment) {
    auto toks = Lexer::Tokenise("-- this is a comment\nSELECT 1;");
    EXPECT_EQ(toks[0].kind, TokenKind::KwSelect);
}

TEST_F(LexerTest, ComparisonOperators) {
    auto toks = Lexer::Tokenise("<= >= <> !=");
    EXPECT_EQ(toks[0].kind, TokenKind::LEq);
    EXPECT_EQ(toks[1].kind, TokenKind::GEq);
    EXPECT_EQ(toks[2].kind, TokenKind::NEq);
    EXPECT_EQ(toks[3].kind, TokenKind::NEq);
}

// ============================================================================
// ParserTest
// ============================================================================

class ParserTest : public ::testing::Test {};

TEST_F(ParserTest, ParsesCreateTable) {
    auto toks = Lexer::Tokenise(
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    auto stmt = Parser::Parse(toks);
    EXPECT_EQ(stmt->kind, StmtKind::CreateTable);
    EXPECT_EQ(stmt->table_name, "users");
    ASSERT_EQ(stmt->columns.size(), 3u);
    EXPECT_EQ(stmt->columns[0].name, "id");
    EXPECT_TRUE(stmt->columns[0].primary_key);
    EXPECT_EQ(stmt->columns[1].type, ColType::Varchar);
    EXPECT_EQ(stmt->columns[1].varchar_len, 64);
}

TEST_F(ParserTest, ParsesInsert) {
    auto toks = Lexer::Tokenise("INSERT INTO users VALUES (1, 'Alice', 30);");
    auto stmt = Parser::Parse(toks);
    EXPECT_EQ(stmt->kind, StmtKind::Insert);
    EXPECT_EQ(stmt->table_name, "users");
    ASSERT_EQ(stmt->insert_rows.size(), 1u);
    ASSERT_EQ(stmt->insert_rows[0].size(), 3u);
}

TEST_F(ParserTest, ParsesSelectStar) {
    auto toks = Lexer::Tokenise("SELECT * FROM users;");
    auto stmt = Parser::Parse(toks);
    EXPECT_EQ(stmt->kind, StmtKind::Select);
    ASSERT_EQ(stmt->select_cols.size(), 1u);
    EXPECT_EQ(stmt->select_cols[0], "*");
    EXPECT_FALSE(stmt->where);
}

TEST_F(ParserTest, ParsesSelectWhere) {
    auto toks = Lexer::Tokenise("SELECT id, name FROM users WHERE age > 20;");
    auto stmt = Parser::Parse(toks);
    EXPECT_EQ(stmt->kind, StmtKind::Select);
    EXPECT_EQ(stmt->select_cols.size(), 2u);
    ASSERT_TRUE(stmt->where);
    EXPECT_EQ(stmt->where->kind, ExprKind::BinaryOp);
    EXPECT_EQ(stmt->where->op, BinOp::Gt);
}

TEST_F(ParserTest, ParsesUpdate) {
    auto toks = Lexer::Tokenise("UPDATE users SET age = 31 WHERE id = 1;");
    auto stmt = Parser::Parse(toks);
    EXPECT_EQ(stmt->kind, StmtKind::Update);
    ASSERT_EQ(stmt->assignments.size(), 1u);
    EXPECT_EQ(stmt->assignments[0].col, "age");
}

TEST_F(ParserTest, ParsesDelete) {
    auto toks = Lexer::Tokenise("DELETE FROM users WHERE id = 2;");
    auto stmt = Parser::Parse(toks);
    EXPECT_EQ(stmt->kind, StmtKind::Delete);
    EXPECT_EQ(stmt->table_name, "users");
    ASSERT_TRUE(stmt->where);
}

TEST_F(ParserTest, ThrowsOnBadSQL) {
    auto toks = Lexer::Tokenise("SELECTT garbage;;");
    EXPECT_THROW(Parser::Parse(toks), ParseError);
}

// ============================================================================
// ExecutorTest
// ============================================================================

class ExecutorTest : public ::testing::Test {
protected:
    static constexpr const char* kCatalog = "test_catalog.dat";
    static constexpr const char* kDir     = ".";

    void SetUp()    override { CleanUp(); }
    void TearDown() override { CleanUp(); }

    void CleanUp() {
        fs::remove(kCatalog);
        fs::remove("./users.idx");
        fs::remove("./users.idx.wal");
        fs::remove("./products.idx");
        fs::remove("./products.idx.wal");
    }

    Executor exec_{kCatalog, kDir};
};

TEST_F(ExecutorTest, CreateTable) {
    auto rs = exec_.ExecSQL(
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    EXPECT_EQ(rs.columns[0], "result");
    EXPECT_FALSE(rs.rows.empty());
}

TEST_F(ExecutorTest, InsertAndSelectStar) {
    exec_.ExecSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    exec_.ExecSQL("INSERT INTO users VALUES (1, 'Alice', 30);");
    exec_.ExecSQL("INSERT INTO users VALUES (2, 'Bob',   25);");

    auto rs = exec_.ExecSQL("SELECT * FROM users;");
    EXPECT_EQ(rs.columns.size(), 3u);
    EXPECT_EQ(rs.rows.size(), 2u);
}

TEST_F(ExecutorTest, SelectProjection) {
    exec_.ExecSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    exec_.ExecSQL("INSERT INTO users VALUES (1, 'Alice', 30);");

    auto rs = exec_.ExecSQL("SELECT name FROM users;");
    ASSERT_EQ(rs.columns.size(), 1u);
    EXPECT_EQ(rs.columns[0], "name");
    ASSERT_EQ(rs.rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rs.rows[0][0]), "Alice");
}

TEST_F(ExecutorTest, SelectWithWhere) {
    exec_.ExecSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    exec_.ExecSQL("INSERT INTO users VALUES (1, 'Alice', 30);");
    exec_.ExecSQL("INSERT INTO users VALUES (2, 'Bob',   25);");
    exec_.ExecSQL("INSERT INTO users VALUES (3, 'Carol', 35);");

    auto rs = exec_.ExecSQL("SELECT * FROM users WHERE age > 28;");
    EXPECT_EQ(rs.rows.size(), 2u);  // Alice (30) and Carol (35)
}

TEST_F(ExecutorTest, Update) {
    exec_.ExecSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    exec_.ExecSQL("INSERT INTO users VALUES (1, 'Alice', 30);");
    exec_.ExecSQL("UPDATE users SET age = 99 WHERE id = 1;");

    auto rs = exec_.ExecSQL("SELECT age FROM users WHERE id = 1;");
    ASSERT_EQ(rs.rows.size(), 1u);
    EXPECT_EQ(std::get<int32_t>(rs.rows[0][0]), 99);
}

TEST_F(ExecutorTest, Delete) {
    exec_.ExecSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    exec_.ExecSQL("INSERT INTO users VALUES (1, 'Alice', 30);");
    exec_.ExecSQL("INSERT INTO users VALUES (2, 'Bob',   25);");
    exec_.ExecSQL("DELETE FROM users WHERE id = 1;");

    auto rs = exec_.ExecSQL("SELECT * FROM users;");
    EXPECT_EQ(rs.rows.size(), 1u);
    EXPECT_EQ(std::get<int32_t>(rs.rows[0][0]), 2);
}

TEST_F(ExecutorTest, DropTable) {
    exec_.ExecSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    exec_.ExecSQL("DROP TABLE users;");
    EXPECT_THROW(exec_.ExecSQL("SELECT * FROM users;"), CatalogError);
}

TEST_F(ExecutorTest, InsertWithExplicitColumns) {
    exec_.ExecSQL("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);");
    exec_.ExecSQL("INSERT INTO users (id, age, name) VALUES (5, 42, 'Dave');");

    auto rs = exec_.ExecSQL("SELECT name, age FROM users WHERE id = 5;");
    ASSERT_EQ(rs.rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rs.rows[0][0]), "Dave");
    EXPECT_EQ(std::get<int32_t>(rs.rows[0][1]), 42);
}
