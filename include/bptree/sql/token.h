/// @file token.h
/// @brief Token kinds and Token struct for the SQL lexer.
///        Nothing fancy — just an enum and a struct. SQL is simple like that :)

#pragma once

#include <string>
#include <string_view>

namespace bptree::sql {

// ---------------------------------------------------------------------------
// Token kinds
// ---------------------------------------------------------------------------
enum class TokenKind {
    // Literals
    IntLit,     ///< 42
    FloatLit,   ///< 3.14
    StrLit,     ///< 'hello'

    // Identifiers
    Ident,      ///< table_name, col

    // Keywords
    KwCreate, KwTable, KwDrop,
    KwInsert,  KwInto,  KwValues,
    KwSelect,  KwFrom,  KwWhere,
    KwUpdate,  KwSet,
    KwDelete,
    KwPrimary, KwKey,
    KwInt,     KwBigInt, KwVarchar, KwFloat,
    KwAnd,     KwOr,     KwNot,
    KwNull,

    // Operators / comparison
    Eq,     ///< =
    NEq,    ///< <>  or  !=
    Lt,     ///< <
    LEq,    ///< <=
    Gt,     ///< >
    GEq,    ///< >=
    Plus,   ///< +
    Minus,  ///< -
    Star,   ///< *

    // Punctuation
    LParen,  ///< (
    RParen,  ///< )
    Comma,   ///< ,
    Semi,    ///< ;
    Dot,     ///< .

    // Special
    Eof,
    Unknown,
};

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------
struct Token {
    TokenKind   kind   = TokenKind::Unknown;
    std::string text;   ///< raw text of the token
    int         line   = 1;

    bool is(TokenKind k)  const noexcept { return kind == k; }
    bool isKeyword()      const noexcept {
        return kind >= TokenKind::KwCreate && kind <= TokenKind::KwNull;
    }
};

} // namespace bptree::sql
