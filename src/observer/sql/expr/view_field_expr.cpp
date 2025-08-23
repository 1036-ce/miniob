#include "sql/expr/view_field_expr.h"
#include "sql/expr/tuple.h"
#include "sql/expr/tuple_cell.h"

RC ViewFieldExpr::get_column(Chunk &chunk, Column &column) {
  return RC::UNIMPLEMENTED;
}

RC ViewFieldExpr::get_value(const Tuple &tuple, Value &value) const {
  return tuple.find_cell(TupleCellSpec(view_name_.c_str(), field_name().c_str()), value);
}

RC ViewFieldExpr::related_tables(vector<const Table *> &tables) const {
  return RC::UNSUPPORTED;
}
