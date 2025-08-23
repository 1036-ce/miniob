/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/subquery_expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"

using namespace std;

string comp2str(CompOp comp)
{
  switch (comp) {
    case CompOp::EQUAL_TO: return "=";
    case CompOp::LESS_EQUAL: return "<=";
    case CompOp::NOT_EQUAL: return "<>";
    case CompOp::LESS_THAN: return "<";
    case CompOp::GREAT_EQUAL: return ">=";
    case CompOp::GREAT_THAN: return ">";
    case CompOp::IS: return "is";
    case CompOp::IS_NOT: return "is not";
    case CompOp::IN: return "in";
    case CompOp::NOT_IN: return "not in";
    case CompOp::LIKE: return "like";
    case CompOp::NOT_LIKE: return "not like";
    default: return "";
  }
}

RC TableFieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

RC TableFieldExpr::related_tables(vector<const Table *> &tables) const
{
  const Table *cur_table = field_.table();
  for (auto table : tables) {
    if (table == cur_table) {
      return RC::SUCCESS;
    }
  }
  tables.push_back(cur_table);
  return RC::SUCCESS;
}

bool TableFieldExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::TABLE_FIELD) {
    return false;
  }
  const auto &other_field_expr = static_cast<const TableFieldExpr &>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC TableFieldExpr::get_column(Chunk &chunk, Column &column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  column.init(value_);
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (value.is_null()) {
    cast_value.set_type(cast_type_);
    cast_value.set_null(true);
    return rc;
  }

  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &result) const
{
  Value value;
  RC    rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

RC CastExpr::try_get_value(Value &result) const
{
  Value value;
  RC    rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, Expression *left, Expression *right)
    : comp_(comp), left_(left), right_(right)
{}
ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC rc = RC::SUCCESS;
  if (comp_ == CompOp::IS) {
    result = left.is_null();
    return rc;
  }
  if (comp_ == CompOp::IS_NOT) {
    result = !left.is_null();
    return rc;
  }
  if (left.is_null() || right.is_null()) {
    result = false;
    return rc;
  }

  if (comp_ == CompOp::LIKE) {
    result = match(left.get_string(), right.get_string());
    return rc;
  }
  if (comp_ == CompOp::NOT_LIKE) {
    result = !match(left.get_string(), right.get_string());
    return rc;
  }

  int cmp_result = left.compare(right);
  result         = false;
  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr   *left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr   *right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    bool value = false;
    RC   rc    = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  if (comp_ == CompOp::IN) {
    return comp_in_handler(tuple, value);
  }
  if (comp_ == CompOp::NOT_IN) {
    return comp_notin_handler(tuple, value);
  }

  Value left_value;
  Value right_value;

  RC rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  bool bool_value = false;

  rc = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

RC ComparisonExpr::eval(Chunk &chunk, vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  } else {
    // TODO: support string compare
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;

  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else if (left_const && !right_const) {
    compare_result<T, true, false>((T *)left.data(), (T *)right.data(), right.count(), result, comp_);
  } else if (!left_const && right_const) {
    compare_result<T, false, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  }
  return rc;
}

RC ComparisonExpr::related_tables(vector<const Table *> &tables) const
{
  RC rc = RC::SUCCESS;
  if (OB_FAIL(rc = left_->related_tables(tables))) {
    return rc;
  }
  return right_->related_tables(tables);
}

RC ComparisonExpr::comp_in_handler(const Tuple &tuple, Value &value) const
{
  RC            rc            = RC::SUCCESS;
  SubQueryExpr *subquyer_expr = static_cast<SubQueryExpr *>(right_.get());

  Value left_value;
  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (subquyer_expr->is_correlated()) {
    // phy_oper has been opened
    auto &phy_oper = subquyer_expr->physical_oper();
    // phy_oper->set_env_tuple(&tuple);

    while (true) {
      rc = phy_oper->next();
      if (rc != RC::SUCCESS) {
        if (rc == RC::RECORD_EOF) {
          break;
        }
        return rc;
      }

      Tuple *subquery_tuple = phy_oper->current_tuple();
      if (subquery_tuple->cell_num() > 1) {
        LOG_WARN("subquery's result must has only one column");
        return RC::UNSUPPORTED;
      }

      Value right_value;
      if (OB_FAIL(rc = subquery_tuple->cell_at(0, right_value))) {
        return rc;
      }

      bool result = left_value.compare(right_value) == 0;
      if (result) {
        value.set_boolean(true);
        return RC::SUCCESS;
      }
    }

    value.set_boolean(false);
    return rc == RC::RECORD_EOF ? RC::SUCCESS : rc;
  } else {
    for (const auto &right_value : subquyer_expr->values()) {
      bool result = left_value.compare(right_value) == 0;
      if (result) {
        value.set_boolean(true);
        return RC::SUCCESS;
      }
    }
    value.set_boolean(false);
    return RC::SUCCESS;
  }
}

RC ComparisonExpr::comp_notin_handler(const Tuple &tuple, Value &value) const
{
  RC rc = comp_in_handler(tuple, value);
  value.set_boolean(!value.get_boolean());
  return rc;
}

bool ComparisonExpr::match(const string& target, const string& pattern) const {
	size_t pos = 0;
	while (pos < target.size() && pos < pattern.size()) {
		if (pattern.at(pos) == '%') {
			break;
		}
		if (pattern.at(pos) == '_') {
			++pos;
			continue;
		}
		if (target.at(pos) != pattern.at(pos)) {
			return false;
		}
		++pos;
	}

	if (pos < pattern.size() && pattern.at(pos) == '%') {
		string pattern_remainder = pattern.substr(pos + 1);
		while (pos <= target.size()) {
			string target_remainder = target.substr(pos);
			if (match(target_remainder, pattern_remainder)) {
				return true;
			}
			++pos;
		}
	}

	if (pos == target.size() && pos == pattern.size()) {
		return true;
	}

	return false;
}

RC ComparisonExpr::to_compareable()
{
  RC rc = RC::SUCCESS;
  if (left_ == nullptr || right_ == nullptr) {
    return rc;
  }

  if (left_->type() == ExprType::SUBQUERY || right_->type() == ExprType::SUBQUERY) {
    return rc;
  }

  if (comp_ == CompOp::LIKE || comp_ == CompOp::NOT_LIKE) {
    return rc;
  }

  // 如果比较的一侧为null，无需cast
  if (left_->type() == ExprType::VALUE) {
    Value val;
    if (OB_FAIL(rc = left_->try_get_value(val))) {
      return rc;
    }
    if (val.is_null()) {
      return rc;
    }
  }
  if (right_->type() == ExprType::VALUE) {
    Value val;
    if (OB_FAIL(rc = right_->try_get_value(val))) {
      return rc;
    }
    if (val.is_null()) {
      return rc;
    }
  }

  auto implicit_cast_cost = [](AttrType from, AttrType to) {
    if (from == to) {
      return 0;
    }
    return DataType::type_instance(from)->cast_cost(to);
  };

  auto make_comparable_expr = [](unique_ptr<Expression> &expr, AttrType target_type) -> RC {
    RC       rc        = RC::SUCCESS;
    ExprType type      = expr->type();
    auto     cast_expr = make_unique<CastExpr>(std::move(expr), target_type);
    if (type == ExprType::VALUE) {
      Value val;
      if (OB_FAIL(rc = cast_expr->try_get_value(val))) {
        LOG_WARN("failed to get value from child", strrc(rc));
        return rc;
      }
      expr = make_unique<ValueExpr>(val);
    } else {
      expr = std::move(cast_expr);
    }
    return RC::SUCCESS;
  };

  if (left_->value_type() != right_->value_type()) {
    auto left_to_right_cost = implicit_cast_cost(left_->value_type(), right_->value_type());
    auto right_to_left_cost = implicit_cast_cost(right_->value_type(), left_->value_type());
    if (left_to_right_cost <= right_to_left_cost && left_to_right_cost != INT32_MAX) {
      if (OB_FAIL(rc = make_comparable_expr(left_, right_->value_type()))) {
        return rc;
      }
    } else if (right_to_left_cost < left_to_right_cost && right_to_left_cost != INT32_MAX) {
      if (OB_FAIL(rc = make_comparable_expr(right_, left_->value_type()))) {
        return rc;
      }
    } else {
      rc = RC::UNSUPPORTED;
      LOG_WARN("unsupported cast from %s to %s", attr_type_to_string(left_->value_type()), attr_type_to_string(right_->value_type()));
      return rc;
    }
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, Expression *left, Expression *right)
    : conjunction_type_(type), left_(left), right_(right)
{}

ConjunctionExpr::ConjunctionExpr(Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : conjunction_type_(type), left_(std::move(left)), right_(std::move(right))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC    rc = RC::SUCCESS;
  Value val;
  if (OB_FAIL(rc = left_->get_value(tuple, val))) {
    return rc;
  }

  if (conjunction_type_ == Type::AND && val.get_boolean() == false) {
    value.set_boolean(false);
    return RC::SUCCESS;
  }
  if (conjunction_type_ == Type::OR && val.get_boolean() == true) {
    value.set_boolean(true);
    return RC::SUCCESS;
  }

  if (OB_FAIL(rc = right_->get_value(tuple, val))) {
    return rc;
  }

  value.set_boolean(val.get_boolean());
  return RC::SUCCESS;
}

RC ConjunctionExpr::try_get_value(Value &value) const
{
  RC    rc = RC::SUCCESS;
  Value val;

  if (OB_FAIL(rc = left_->try_get_value(val))) {
    return rc;
  }

  if (conjunction_type_ == Type::AND && val.get_boolean() == false) {
    value.set_boolean(false);
    return RC::SUCCESS;
  }
  if (conjunction_type_ == Type::OR && val.get_boolean() == true) {
    value.set_boolean(true);
    return RC::SUCCESS;
  }

  if (OB_FAIL(rc = right_->try_get_value(val))) {
    return rc;
  }

  value.set_boolean(val.get_boolean());
  return RC::SUCCESS;
}

RC ConjunctionExpr::related_tables(vector<const Table *> &tables) const
{
  RC rc = RC::SUCCESS;
  if (OB_FAIL(rc = left_->related_tables(tables))) {
    return rc;
  }
  return right_->related_tables(tables);
}

auto ConjunctionExpr::flatten(ExprType type) -> vector<unique_ptr<Expression> *>
{
  vector<unique_ptr<Expression> *> ret;

  queue<unique_ptr<Expression> *> que;
  que.push(&left_);
  que.push(&right_);

  while (!que.empty()) {
    auto expr = que.front();
    que.pop();

    if ((*expr)->type() == type) {
      ret.push_back(expr);
      continue;
    }

    if ((*expr)->type() == ExprType::COMPARISON) {
      auto                    comp_expr = static_cast<ComparisonExpr *>((*expr).get());
      unique_ptr<Expression> *left      = &comp_expr->left();
      unique_ptr<Expression> *right     = &comp_expr->right();
      que.push(left);
      que.push(right);
    } else if ((*expr)->type() == ExprType::CONJUNCTION) {
      auto                    conjunction_expr = static_cast<ConjunctionExpr *>((*expr).get());
      unique_ptr<Expression> *left             = &conjunction_expr->left_;
      unique_ptr<Expression> *right            = &conjunction_expr->right_;
      que.push(left);
      que.push(right);
    } else {
      // ASSERT(false, "Not Supported");
      continue;
    }
  }
  return ret;
}

auto ConjunctionExpr::extract(const vector<const Table *> &target_tables) -> unique_ptr<Expression>
{

  auto equal = [](const vector<const Table *> &lhs, const vector<const Table *> &rhs) -> bool {
    if (lhs.size() != rhs.size()) {
      return false;
    }

    for (auto table : lhs) {
      if (std::find(rhs.begin(), rhs.end(), table) == rhs.end()) {
        return false;
      }
    }
    return true;
  };

  auto ret              = this->copy();
  auto conjunction_expr = static_cast<ConjunctionExpr *>(ret.get());

  vector<unique_ptr<Expression> *> exprs = conjunction_expr->flatten(ExprType::COMPARISON);
  vector<const Table *>            tables;

  for (auto expr : exprs) {
    if ((*expr)->type() == ExprType::COMPARISON) {
      auto comp_expr = static_cast<ComparisonExpr *>(expr->get());

      tables.clear();
      comp_expr->related_tables(tables);

      if (!equal(target_tables, tables)) {
        *expr = make_unique<ValueExpr>(Value(true));
      }
    }
  }
  return conjunction_expr->simplify();
}

auto ConjunctionExpr::simplify() -> unique_ptr<Expression>
{
  unique_ptr<Expression> left, right;
  Value                  val;

  if (left_->type() == ExprType::CONJUNCTION) {
    auto conjunction_expr = static_cast<ConjunctionExpr *>(left_.get());
    left.reset(conjunction_expr->simplify().release());
  }

  if (right_->type() == ExprType::CONJUNCTION) {
    auto conjunction_expr = static_cast<ConjunctionExpr *>(right_.get());
    right.reset(conjunction_expr->simplify().release());
  }

  if (OB_FAIL(left->try_get_value(val)) && OB_FAIL(right->try_get_value(val))) {
    return make_unique<ConjunctionExpr>(conjunction_type_, std::move(left), std::move(right));
  }

  auto func = [this](Value val, unique_ptr<Expression> &other) -> unique_ptr<Expression> {
    if (conjunction_type_ == Type::AND) {
      return true == val.get_boolean() ? std::move(other) : make_unique<ValueExpr>(Value(false));
    } else {
      return false == val.get_boolean() ? std::move(other) : make_unique<ValueExpr>(Value(true));
    }
  };

  if (OB_SUCC(left->try_get_value(val))) {
    return func(val, right);
  }
  if (OB_SUCC(right->try_get_value(val))) {
    return func(val, left);
  }
  return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         right_->equal(*other_arith_expr.right_);
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  value.set_type(target_type);

  switch (arithmetic_type_) {
    case Type::ADD: {
      Value::add(left_value, right_value, value);
    } break;

    case Type::SUB: {
      Value::subtract(left_value, right_value, value);
    } break;

    case Type::MUL: {
      Value::multiply(left_value, right_value, value);
    } break;

    case Type::DIV: {
      Value::divide(left_value, right_value, value);
    } break;

    case Type::NEGATIVE: {
      Value::negative(left_value, value);
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
    case Type::ADD: {
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
    } break;
    case Type::SUB:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::MUL:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::DIV:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::NEGATIVE:
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    default: rc = RC::UNIMPLEMENTED; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_) {
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), max(left_column.count(), right_column.count()));
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::related_tables(vector<const Table *> &tables) const
{
  RC rc = RC::SUCCESS;
  if (OB_FAIL(rc = left_->related_tables(tables))) {
    return rc;
  }
  return right_->related_tables(tables);
}
////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, unique_ptr<Expression> child)
    : aggregate_name_(aggregate_name), child_(std::move(child))
{}

////////////////////////////////////////////////////////////////////////////////
AggregateExpr::AggregateExpr(Type type, Expression *child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{}

RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
    case Type::COUNT: {
      aggregator = make_unique<CountAggregator>();
      break;
    }
    case Type::SUM: {
      aggregator = make_unique<SumAggregator>();
      break;
    }
    case Type::AVG: {
      aggregator = make_unique<AvgAggregator>();
      break;
    }
    case Type::MAX: {
      aggregator = make_unique<MaxAggregator>();
      break;
    }
    case Type::MIN: {
      aggregator = make_unique<MinAggregator>();
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");
      break;
    }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

RC AggregateExpr::type_from_string(const char *type_str, AggregateExpr::Type &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}
