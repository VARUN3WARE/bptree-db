/// @file ast.cpp
/// @brief Expr factory method implementations.

#include "bptree/sql/ast.h"

namespace bptree::sql {

ExprPtr Expr::MakeInt(int64_t v) {
    auto e = std::make_unique<Expr>();
    e->kind    = ExprKind::IntLit;
    e->int_val = v;
    return e;
}

ExprPtr Expr::MakeFloat(double v) {
    auto e = std::make_unique<Expr>();
    e->kind    = ExprKind::FloatLit;
    e->flt_val = v;
    return e;
}

ExprPtr Expr::MakeStr(const std::string& s) {
    auto e = std::make_unique<Expr>();
    e->kind    = ExprKind::StrLit;
    e->str_val = s;
    return e;
}

ExprPtr Expr::MakeCol(const std::string& col) {
    auto e = std::make_unique<Expr>();
    e->kind    = ExprKind::ColRef;
    e->str_val = col;
    return e;
}

ExprPtr Expr::MakeBin(BinOp op, ExprPtr l, ExprPtr r) {
    auto e = std::make_unique<Expr>();
    e->kind  = ExprKind::BinaryOp;
    e->op    = op;
    e->left  = std::move(l);
    e->right = std::move(r);
    return e;
}

ExprPtr Expr::MakeNull() {
    auto e = std::make_unique<Expr>();
    e->kind = ExprKind::Null;
    return e;
}

} // namespace bptree::sql
