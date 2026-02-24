/// @file lexer.h
/// @brief Hand-written SQL lexer. One pass, no regex, no regrets.

#pragma once

#include "bptree/sql/token.h"

#include <string>
#include <vector>

namespace bptree::sql {

class Lexer {
public:
    /// Tokenise the entire SQL string. Always terminates with TokenKind::Eof.
    static std::vector<Token> Tokenise(const std::string& src);

private:
    explicit Lexer(const std::string& src) : src_(src) {}

    std::vector<Token> run();

    // -- helpers --
    char  peek(int offset = 0) const;
    char  advance();
    bool  at_end()      const noexcept { return pos_ >= src_.size(); }
    void  skip_ws();
    Token read_string();
    Token read_number();
    Token read_word();       // ident or keyword
    Token make(TokenKind k, std::string text = "");

    static TokenKind keyword_kind(const std::string& w);

    const std::string& src_;
    size_t pos_  = 0;
    int    line_ = 1;
};

} // namespace bptree::sql
