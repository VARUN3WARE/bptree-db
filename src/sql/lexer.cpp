/// @file lexer.cpp
/// @brief SQL lexer implementation. Life's too short for flex/bison. :)

#include "bptree/sql/lexer.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <stdexcept>
#include <unordered_map>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// Static keyword table
// ---------------------------------------------------------------------------

static const std::unordered_map<std::string, TokenKind> kKeywords = {
    {"CREATE",  TokenKind::KwCreate},
    {"TABLE",   TokenKind::KwTable},
    {"DROP",    TokenKind::KwDrop},
    {"INSERT",  TokenKind::KwInsert},
    {"INTO",    TokenKind::KwInto},
    {"VALUES",  TokenKind::KwValues},
    {"SELECT",  TokenKind::KwSelect},
    {"FROM",    TokenKind::KwFrom},
    {"WHERE",   TokenKind::KwWhere},
    {"UPDATE",  TokenKind::KwUpdate},
    {"SET",     TokenKind::KwSet},
    {"DELETE",  TokenKind::KwDelete},
    {"PRIMARY", TokenKind::KwPrimary},
    {"KEY",     TokenKind::KwKey},
    {"INT",     TokenKind::KwInt},
    {"BIGINT",  TokenKind::KwBigInt},
    {"VARCHAR", TokenKind::KwVarchar},
    {"FLOAT",   TokenKind::KwFloat},
    {"AND",     TokenKind::KwAnd},
    {"OR",      TokenKind::KwOr},
    {"NOT",     TokenKind::KwNot},
    {"NULL",    TokenKind::KwNull},
};

// ---------------------------------------------------------------------------
// Public entry-point
// ---------------------------------------------------------------------------

std::vector<Token> Lexer::Tokenise(const std::string& src) {
    Lexer l(src);
    return l.run();
}

// ---------------------------------------------------------------------------
// Core loop
// ---------------------------------------------------------------------------

std::vector<Token> Lexer::run() {
    std::vector<Token> tokens;

    while (!at_end()) {
        skip_ws();
        if (at_end()) break;

        char c = peek();

        // Single-line comment
        if (c == '-' && peek(1) == '-') {
            while (!at_end() && peek() != '\n') advance();
            continue;
        }

        if (c == '\'')           { tokens.push_back(read_string()); continue; }
        if (std::isdigit(c))     { tokens.push_back(read_number()); continue; }
        if (std::isalpha(c) || c == '_') { tokens.push_back(read_word()); continue; }

        // Operators / punctuation
        advance();
        switch (c) {
            case '(':  tokens.push_back(make(TokenKind::LParen, "(")); break;
            case ')':  tokens.push_back(make(TokenKind::RParen, ")")); break;
            case ',':  tokens.push_back(make(TokenKind::Comma,  ",")); break;
            case ';':  tokens.push_back(make(TokenKind::Semi,   ";")); break;
            case '.':  tokens.push_back(make(TokenKind::Dot,    ".")); break;
            case '+':  tokens.push_back(make(TokenKind::Plus,   "+")); break;
            case '-':  tokens.push_back(make(TokenKind::Minus,  "-")); break;
            case '*':  tokens.push_back(make(TokenKind::Star,   "*")); break;
            case '=':  tokens.push_back(make(TokenKind::Eq,     "=")); break;
            case '<':
                if (!at_end() && peek() == '=') { advance(); tokens.push_back(make(TokenKind::LEq, "<=")); }
                else if (!at_end() && peek() == '>') { advance(); tokens.push_back(make(TokenKind::NEq, "<>")); }
                else tokens.push_back(make(TokenKind::Lt, "<"));
                break;
            case '>':
                if (!at_end() && peek() == '=') { advance(); tokens.push_back(make(TokenKind::GEq, ">=")); }
                else tokens.push_back(make(TokenKind::Gt, ">"));
                break;
            case '!':
                if (!at_end() && peek() == '=') { advance(); tokens.push_back(make(TokenKind::NEq, "!=")); }
                else tokens.push_back(make(TokenKind::Unknown, "!"));
                break;
            default:
                tokens.push_back(make(TokenKind::Unknown, std::string(1, c)));
        }
    }

    tokens.push_back(make(TokenKind::Eof, ""));
    return tokens;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

char Lexer::peek(int offset) const {
    size_t idx = pos_ + static_cast<size_t>(offset);
    return idx < src_.size() ? src_[idx] : '\0';
}

char Lexer::advance() {
    char c = src_[pos_++];
    if (c == '\n') ++line_;
    return c;
}

void Lexer::skip_ws() {
    while (!at_end() && std::isspace(static_cast<unsigned char>(peek()))) advance();
}

Token Lexer::read_string() {
    advance();  // consume opening '
    std::string val;
    while (!at_end() && peek() != '\'') {
        char c = advance();
        if (c == '\\' && !at_end()) val += advance();  // basic escape
        else val += c;
    }
    if (!at_end()) advance();  // consume closing '
    return {TokenKind::StrLit, val, line_};
}

Token Lexer::read_number() {
    std::string val;
    while (!at_end() && std::isdigit(static_cast<unsigned char>(peek()))) val += advance();
    if (!at_end() && peek() == '.') {
        val += advance();
        while (!at_end() && std::isdigit(static_cast<unsigned char>(peek()))) val += advance();
        return {TokenKind::FloatLit, val, line_};
    }
    return {TokenKind::IntLit, val, line_};
}

Token Lexer::read_word() {
    std::string raw;
    while (!at_end() && (std::isalnum(static_cast<unsigned char>(peek())) || peek() == '_'))
        raw += advance();

    // upper-case for keyword comparison
    std::string up = raw;
    std::transform(up.begin(), up.end(), up.begin(), ::toupper);

    auto it = kKeywords.find(up);
    if (it != kKeywords.end()) return {it->second, up, line_};
    return {TokenKind::Ident, raw, line_};
}

Token Lexer::make(TokenKind k, std::string text) {
    return {k, std::move(text), line_};
}

TokenKind Lexer::keyword_kind(const std::string& w) {
    auto it = kKeywords.find(w);
    return it != kKeywords.end() ? it->second : TokenKind::Ident;
}

} // namespace bptree::sql
