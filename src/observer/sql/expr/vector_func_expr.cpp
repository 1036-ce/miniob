#include "sql/expr/vector_func_expr.h"
#include "common/type/vector_type.h"

RC VectorFuncExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_child_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_child_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_value(left_value, right_value, value);
}

RC VectorFuncExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  switch (func_type_) {
    case Type::L2_DISTANCE: {
      rc = VectorType{}.l2_distance(left_value, right_value, value);
    } break;

    case Type::COSINE_DISTANCE: {
      rc = VectorType{}.cosine_distance(left_value, right_value, value);
    } break;

    case Type::INNER_PRODUCT: {
      rc = VectorType{}.inner_product(left_value, right_value, value);
    } break;
    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported function type. %d", func_type_);
    } break;
  }
  return rc;
}

RC VectorFuncExpr::get_column(Chunk &chunk, Column &column) { return RC::SUCCESS; }

RC VectorFuncExpr::to_computable()
{
  RC rc = RC::SUCCESS;

  if (left_child_->type() == ExprType::VALUE && left_child_->value_type() == AttrType::CHARS) {
    ValueExpr *left_val_expr = static_cast<ValueExpr *>(left_child_.get());
    Value      left_val;
    left_val_expr->get_value(left_val);
    Value vector_val;
    if (OB_FAIL(rc = VectorType{}.set_value_from_str(vector_val, left_val.get_string()))) {
      return rc;
    }
    left_child_.reset(new ValueExpr(vector_val));
  }

  if (right_child_->type() == ExprType::VALUE && right_child_->value_type() == AttrType::CHARS) {
    ValueExpr *right_val_expr = static_cast<ValueExpr *>(right_child_.get());
    Value      right_val;
    right_val_expr->get_value(right_val);
    Value vector_val;
    if (OB_FAIL(rc = VectorType{}.set_value_from_str(vector_val, right_val.get_string()))) {
      return rc;
    }
    right_child_.reset(new ValueExpr(vector_val));
  }

  if (left_child_->value_type() != AttrType::VECTORS || right_child_->value_type() != AttrType::VECTORS) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  if (left_child_->value_length() != right_child_->value_length()) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  return RC::SUCCESS;
}

RC VectorFuncExpr::type_from_string(const char *type_str, Type &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "l2_distance")) {
    type = Type::L2_DISTANCE;
  } else if (0 == strcasecmp(type_str, "cosine_distance")) {
    type = Type::COSINE_DISTANCE;
  } else if (0 == strcasecmp(type_str, "inner_product")) {
    type = Type::INNER_PRODUCT;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}
