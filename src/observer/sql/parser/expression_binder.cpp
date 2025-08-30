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
// Created by Wangyunlai on 2024/05/29.
//

#include "common/log/log.h"
#include "common/lang/string.h"
#include "common/lang/ranges.h"
#include "sql/parser/expression_binder.h"
#include "sql/expr/expression_iterator.h"
#include "sql/expr/subquery_expression.h"
#include "sql/expr/vector_func_expr.h"
#include "sql/expr/view_field_expr.h"

using namespace common;

static void wildcard_fields(
    const DataSource &ds, const string &ds_ref_name, vector<unique_ptr<Expression>> &expressions)
{
  if (ds.table()) {
    Table           *table      = ds.table();
    const TableMeta &table_meta = table->table_meta();
    const int        field_num  = table_meta.field_num();

    // 通配符会扩展为所有**可见**的field
    for (int i = table_meta.unvisible_field_num(); i < field_num; i++) {
      Field           field(table, table_meta.field(i));
      TableFieldExpr *field_expr = new TableFieldExpr(field, ds_ref_name);

      string expr_name = field.field_name();
      field_expr->set_name(expr_name);
      expressions.emplace_back(field_expr);
    }
  } else {
    View       *view        = ds.view();
    const auto &field_metas = view->field_metas();
    for (const auto &field_meta : field_metas) {
      ViewFieldExpr *view_field_expr = new ViewFieldExpr(view->name(), field_meta);

      string expr_name = field_meta.name();
      view_field_expr->set_name(expr_name);
      expressions.emplace_back(view_field_expr);
    }
  }
}

RC ExpressionBinder::bind_expression(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  switch (expr->type()) {
    case ExprType::STAR: {
      return bind_star_expression(expr, bound_expressions);
    } break;

    case ExprType::UNBOUND_FIELD: {
      return bind_unbound_field_expression(expr, bound_expressions);
    } break;

    case ExprType::UNBOUND_AGGREGATION: {
      return bind_aggregate_expression(expr, bound_expressions);
    } break;

    case ExprType::UNBOUND_VECTOR_FUNC: {
      return bind_vectorfunc_expression(expr, bound_expressions);
    } break;

    case ExprType::TABLE_FIELD: {
      return bind_table_field_expression(expr, bound_expressions);
    } break;

    case ExprType::VIEW_FIELD: {
      return bind_view_field_expression(expr, bound_expressions);
    } break;

    case ExprType::VALUE: {
      return bind_value_expression(expr, bound_expressions);
    } break;

    case ExprType::CAST: {
      return bind_cast_expression(expr, bound_expressions);
    } break;

    case ExprType::COMPARISON: {
      return bind_comparison_expression(expr, bound_expressions);
    } break;

    case ExprType::CONJUNCTION: {
      return bind_conjunction_expression(expr, bound_expressions);
    } break;

    case ExprType::ARITHMETIC: {
      return bind_arithmetic_expression(expr, bound_expressions);
    } break;

    case ExprType::SUBQUERY: {
      return bind_subquery_expression(expr, bound_expressions);
    } break;

    case ExprType::AGGREGATION: {
      ASSERT(false, "shouldn't be here");
    } break;

    case ExprType::VECTOR_FUNC: {
      ASSERT(false, "shouldn't be here");
    } break;

    default: {
      LOG_WARN("unknown expression type: %d", static_cast<int>(expr->type()));
      return RC::INTERNAL;
    }
  }
  return RC::INTERNAL;
}

RC ExpressionBinder::bind_star_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto star_expr = static_cast<StarExpr *>(expr.get());

  const char *ds_name = star_expr->table_name();
  if (!is_blank(ds_name) && 0 != strcmp(ds_name, "*")) {
    auto ds = context_.find_current_data_source(ds_name);
    if (!ds.is_valid()) {
      LOG_INFO("no such table in from list: %s", ds_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    wildcard_fields(ds, ds_name, bound_expressions);
  } else {
    const auto &ds_names = context_.current_ds_names();
    for (const auto &name : ds_names) {
      auto ds = context_.find_current_data_source(name.c_str());
      wildcard_fields(ds, name, bound_expressions);
    }
  }

  /*   vector<DataSource> ds_to_wildcard;
   *   const char *ds_name = star_expr->table_name();
   *   if (!is_blank(ds_name) && 0 != strcmp(ds_name, "*")) {
   *     auto ds = context_.find_current_data_source(ds_name);
   *     if (!ds.is_valid()) {
   *       LOG_INFO("no such table in from list: %s", ds_name);
   *       return RC::SCHEMA_TABLE_NOT_EXIST;
   *     }
   *
   *     // tables_to_wildcard.push_back(table);
   *     ds_to_wildcard.emplace_back(ds);
   *   } else {
   *     vector<DataSource> all_ds = context_.current_data_sources();
   *     ds_to_wildcard.insert(ds_to_wildcard.end(), all_ds.begin(), all_ds.end());
   *   }
   *   for (const auto &ds : ds_to_wildcard) {
   *     wildcard_fields(ds, bound_expressions);
   *   } */

  return RC::SUCCESS;
}

RC ExpressionBinder::bind_unbound_field_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto unbound_field_expr = static_cast<UnboundFieldExpr *>(expr.get());

  const char *ds_name    = unbound_field_expr->table_name();
  const char *field_name = unbound_field_expr->field_name();

  DataSource ds;
  if (is_blank(ds_name)) {
    if (context_.current_data_sources().size() != 1) {
      LOG_INFO("cannot determine table for field: %s", field_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    ds = context_.current_data_sources()[0];
  } else {
    ds = context_.find_current_data_source(ds_name);
    if (!ds.is_valid()) {
      ds = context_.find_outer_data_source(ds_name);
      if (!ds.is_valid()) {
        LOG_INFO("no such table in BinderContext: %s", ds_name);
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
      // context_.add_used_outer_data_source(ds);
      context_.add_used_outer_data_source(ds_name);
    }
  }

  if (0 == strcmp(field_name, "*")) {
    // wildcard_fields(ds, bound_expressions);
    wildcard_fields(ds, ds_name, bound_expressions);
  } else {
    if (ds.table() != nullptr) {
      Table           *table      = ds.table();
      const FieldMeta *field_meta = table->table_meta().field(field_name);
      if (nullptr == field_meta) {
        LOG_INFO("no such field in table: %s.%s", ds_name, field_name);
        return RC::SCHEMA_FIELD_MISSING;
      }

      string          ds_ref_name = is_blank(ds_name) ? context_.current_ds_names().at(0) : ds_name;
      Field           field(table, field_meta);
      TableFieldExpr *field_expr = new TableFieldExpr(field, ds_ref_name);

      if (!unbound_field_expr->alias_name().empty()) {
        field_expr->set_name(unbound_field_expr->alias_name());
      } else {
        field_expr->set_name(field_name);
      }
      bound_expressions.emplace_back(field_expr);
    } else {
      View                *view       = ds.view();
      const ViewFieldMeta *field_meta = view->field_meta(field_name);
      if (nullptr == field_meta) {
        LOG_INFO("no such field in view: %s.%s", ds_name, field_name);
        return RC::SCHEMA_FIELD_MISSING;
      }
      ViewFieldExpr *view_field_expr = new ViewFieldExpr(view->name(), *field_meta);
      if (!unbound_field_expr->alias_name().empty()) {
        view_field_expr->set_name(unbound_field_expr->alias_name());
      } else {
        view_field_expr->set_name(field_name);
      }
      bound_expressions.emplace_back(view_field_expr);
    }
  }

  return RC::SUCCESS;
}

RC ExpressionBinder::bind_table_field_expression(
    unique_ptr<Expression> &field_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  bound_expressions.emplace_back(std::move(field_expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_view_field_expression(
    unique_ptr<Expression> &field_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  bound_expressions.emplace_back(std::move(field_expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_value_expression(
    unique_ptr<Expression> &value_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (!value_expr->alias_name().empty()) {
    value_expr->set_name(value_expr->alias_name());
  }
  bound_expressions.emplace_back(std::move(value_expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_cast_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto cast_expr = static_cast<CastExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &child_expr = cast_expr->child();

  RC rc = bind_expression(child_expr, child_bound_expressions);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid children number of cast expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &child = child_bound_expressions[0];
  if (child.get() == child_expr.get()) {
    return RC::SUCCESS;
  }

  child_expr.reset(child.release());
  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_comparison_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto comparison_expr = static_cast<ComparisonExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr  = comparison_expr->left();
  unique_ptr<Expression>        &right_expr = comparison_expr->right();

  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  child_bound_expressions.clear();
  rc = bind_expression(right_expr, child_bound_expressions);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid right children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &right = child_bound_expressions[0];
  if (right.get() != right_expr.get()) {
    right_expr.reset(right.release());
  }

  child_bound_expressions.clear();

  // 在comparison的left和right绑定完成后，做to_compareable
  if (OB_FAIL(rc = comparison_expr->to_compareable())) {
    return rc;
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_conjunction_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr  = conjunction_expr->left();
  unique_ptr<Expression>        &right_expr = conjunction_expr->right();

  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  child_bound_expressions.clear();
  rc = bind_expression(right_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid right children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &right = child_bound_expressions[0];
  if (right.get() != right_expr.get()) {
    right_expr.reset(right.release());
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_arithmetic_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto arithmetic_expr = static_cast<ArithmeticExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr  = arithmetic_expr->left();
  unique_ptr<Expression>        &right_expr = arithmetic_expr->right();

  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  if (right_expr) {
    child_bound_expressions.clear();
    rc = bind_expression(right_expr, child_bound_expressions);
    if (OB_FAIL(rc)) {
      return rc;
    }

    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid right children number of comparison expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    unique_ptr<Expression> &right = child_bound_expressions[0];
    if (right.get() != right_expr.get()) {
      right_expr.reset(right.release());
    }
  }

  // 在arithmetic_expr的left和right绑定完成后，做to_computable
  if (OB_FAIL(rc = arithmetic_expr->to_computable())) {
    return rc;
  }

  if (!expr->alias_name().empty()) {
    expr->set_name(expr->alias_name());
  }
  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC check_aggregate_expression(AggregateExpr &expression)
{
  // 必须有一个子表达式
  Expression *child_expression = expression.child().get();
  if (nullptr == child_expression) {
    LOG_WARN("child expression of aggregate expression is null");
    return RC::INVALID_ARGUMENT;
  }

  // 校验数据类型与聚合类型是否匹配
  AggregateExpr::Type aggregate_type   = expression.aggregate_type();
  AttrType            child_value_type = child_expression->value_type();
  switch (aggregate_type) {
    case AggregateExpr::Type::SUM:
    case AggregateExpr::Type::AVG: {
      // 仅支持数值类型
      if (child_value_type != AttrType::INTS && child_value_type != AttrType::FLOATS) {
        LOG_WARN("invalid child value type for aggregate expression: %d", static_cast<int>(child_value_type));
        return RC::INVALID_ARGUMENT;
      }
    } break;

    case AggregateExpr::Type::COUNT:
    case AggregateExpr::Type::MAX:
    case AggregateExpr::Type::MIN: {
      // 任何类型都支持
    } break;
  }

  // 子表达式中不能再包含聚合表达式
  function<RC(unique_ptr<Expression> &)> check_aggregate_expr = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      LOG_WARN("aggregate expression cannot be nested");
      return RC::INVALID_ARGUMENT;
    }
    rc = ExpressionIterator::iterate_child_expr(*expr, check_aggregate_expr);
    return rc;
  };

  RC rc = ExpressionIterator::iterate_child_expr(expression, check_aggregate_expr);

  return rc;
}

RC ExpressionBinder::bind_aggregate_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto                unbound_aggregate_expr = static_cast<UnboundAggregateExpr *>(expr.get());
  const char         *aggregate_name         = unbound_aggregate_expr->aggregate_name();
  AggregateExpr::Type aggregate_type;
  RC                  rc = AggregateExpr::type_from_string(aggregate_name, aggregate_type);
  if (OB_FAIL(rc)) {
    LOG_WARN("invalid aggregate name: %s", aggregate_name);
    return rc;
  }

  unique_ptr<Expression>        &child_expr = unbound_aggregate_expr->child();
  vector<unique_ptr<Expression>> child_bound_expressions;

  if (child_expr->type() == ExprType::STAR && aggregate_type == AggregateExpr::Type::COUNT) {
    ValueExpr *value_expr = new ValueExpr(Value(1));
    child_expr.reset(value_expr);
  } else {
    rc = bind_expression(child_expr, child_bound_expressions);
    if (OB_FAIL(rc)) {
      return rc;
    }

    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid children number of aggregate expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    if (child_bound_expressions[0].get() != child_expr.get()) {
      child_expr.reset(child_bound_expressions[0].release());
    }
  }

  auto aggregate_expr = make_unique<AggregateExpr>(aggregate_type, std::move(child_expr));
  if (!unbound_aggregate_expr->alias_name().empty()) {
    aggregate_expr->set_name(unbound_aggregate_expr->alias_name());
  } else {
    aggregate_expr->set_name(unbound_aggregate_expr->name());
  }
  rc = check_aggregate_expression(*aggregate_expr);
  if (OB_FAIL(rc)) {
    return rc;
  }

  bound_expressions.emplace_back(std::move(aggregate_expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_vectorfunc_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto unbound_vector_func_expr = static_cast<UnboundVectorFuncExpr *>(expr.get());
  const char* function_name = unbound_vector_func_expr->function_name();
  VectorFuncExpr::Type func_type;
  RC                  rc = VectorFuncExpr::type_from_string(function_name, func_type);
  if (OB_FAIL(rc)) {
    LOG_WARN("invalid vector function name: %s", function_name);
    return rc;
  }

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr  = unbound_vector_func_expr->left_child();
  unique_ptr<Expression>        &right_expr = unbound_vector_func_expr->right_child();

  rc = bind_expression(left_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  child_bound_expressions.clear();
  rc = bind_expression(right_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid right children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &right = child_bound_expressions[0];
  if (right.get() != right_expr.get()) {
    right_expr.reset(right.release());
  }

  auto vector_func_expr = make_unique<VectorFuncExpr>(func_type, std::move(left_expr), std::move(right_expr));
  if (!unbound_vector_func_expr->alias_name().empty()) {
    vector_func_expr->set_name(unbound_vector_func_expr->alias_name());
  } else {
    vector_func_expr->set_name(unbound_vector_func_expr->name());
  }

  // 在vector_func_expr的left和right绑定完成后，做to_computable
  if (OB_FAIL(rc = vector_func_expr->to_computable())) {
    return rc;
  }

  bound_expressions.emplace_back(std::move(vector_func_expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_subquery_expression(
    unique_ptr<Expression> &subquery_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  RC            rc   = RC::SUCCESS;
  auto          expr = static_cast<SubQueryExpr *>(subquery_expr.get());
  SubQueryExpr *ret  = nullptr;
  if (expr->sql_node()) {
    ret = new SubQueryExpr{expr->sql_node().release()};
    // if (OB_FAIL(rc = ret->build_select_stmt(context_.db(), context_.query_tables()))) {
    if (OB_FAIL(rc = ret->build_select_stmt(context_))) {
      return rc;
    }
  } else {
    ret = new SubQueryExpr{expr->values()};
  }
  if (!expr->alias_name().empty()) {
    ret->set_name(expr->alias_name());
  }
  bound_expressions.emplace_back(ret);
  return rc;
}
