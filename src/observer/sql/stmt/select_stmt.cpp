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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"
#include "sql/expr/expression_iterator.h"

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  RC            rc = RC::SUCCESS;
  BinderContext binder_context;

  // collect tables in `from` statement
  unordered_map<string, Table *> table_map;
  if (OB_FAIL(rc = collect_tables(db, select_sql.table_refs.get(), table_map, binder_context))) {
    return rc;
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);

  unique_ptr<BoundTable> tables = bind_tables(table_map, expression_binder, select_sql.table_refs.get());

  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // create groupby clause
  vector<unique_ptr<Expression>> group_by_expressions;
  if (select_sql.group_by) {
    for (unique_ptr<Expression> &expression : select_sql.group_by->exprs) {
      RC rc = expression_binder.bind_expression(expression, group_by_expressions);
      if (OB_FAIL(rc)) {
        LOG_INFO("bind expression failed. rc=%s", strrc(rc));
        return rc;
      }
    }
  } 

  // create orderby clause
  vector<unique_ptr<Expression>> orderby_expressions;
  vector<unique_ptr<OrderBy>>    orderby;
  for (auto &orderby_entry : select_sql.order_by) {
    RC rc = expression_binder.bind_expression(orderby_entry->expr, orderby_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
    ASSERT(orderby_expressions.size() == 1, "bound orderby expressions' size must equal to 1");
    OrderBy *new_orderby_entry = new OrderBy;
    new_orderby_entry->is_asc  = orderby_entry->is_asc;
    new_orderby_entry->expr.reset(orderby_expressions.front().release());
    orderby.emplace_back(new_orderby_entry);
    orderby_expressions.clear();
  }

  Table *default_table = nullptr;
  if (table_map.size() == 1) {
    default_table = table_map.begin()->second;
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  rc = FilterStmt::create(db, default_table, &table_map, std::move(select_sql.condition), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // create filter statement in 'having' statement
  FilterStmt *having_filter_stmt = nullptr;
  if (select_sql.group_by) {
    rc = FilterStmt::create(
        db, default_table, &table_map, std::move(select_sql.group_by->having_predicate), having_filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->table_tree_.reset(tables.release());
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->having_filter_stmt_ = having_filter_stmt;
  select_stmt->order_by_.swap(orderby);
  stmt = select_stmt;
  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt, BinderContext &binder_context)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  RC                             rc = RC::SUCCESS;
  unordered_map<string, Table *> table_map;

  // collect tables in `from` statement
  if (OB_FAIL(rc = collect_tables(db, select_sql.table_refs.get(), table_map, binder_context))) {
    return rc;
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);

  unique_ptr<BoundTable> tables = bind_tables(table_map, expression_binder, select_sql.table_refs.get());

  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // create groupby clause
  vector<unique_ptr<Expression>> group_by_expressions;
  if (select_sql.group_by) {
    for (unique_ptr<Expression> &expression : select_sql.group_by->exprs) {
      RC rc = expression_binder.bind_expression(expression, group_by_expressions);
      if (OB_FAIL(rc)) {
        LOG_INFO("bind expression failed. rc=%s", strrc(rc));
        return rc;
      }
    }
  }

  // create orderby clause
  vector<unique_ptr<Expression>> orderby_expressions;
  vector<unique_ptr<OrderBy>>    orderby;
  for (auto &orderby_entry : select_sql.order_by) {
    RC rc = expression_binder.bind_expression(orderby_entry->expr, orderby_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
    ASSERT(orderby_expressions.size() == 1, "bound orderby expressions' size must equal to 1");
    OrderBy *new_orderby_entry = new OrderBy;
    new_orderby_entry->is_asc  = orderby_entry->is_asc;
    new_orderby_entry->expr.reset(orderby_expressions.front().release());
    orderby.emplace_back(new_orderby_entry);
  }

  // create filter statement in `where` clause
  FilterStmt *filter_stmt = nullptr;
  rc                      = FilterStmt::create(db, binder_context, std::move(select_sql.condition), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->table_tree_.reset(tables.release());
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->order_by_.swap(orderby);
  stmt = select_stmt;
  return RC::SUCCESS;
}

auto SelectStmt::collect_tables(
    Db *db, UnboundTable *unbound_table, unordered_map<string, Table *> &table_map, BinderContext &binder_context) -> RC
{
  if (UnboundSingleTable *single_table = dynamic_cast<UnboundSingleTable *>(unbound_table); single_table != nullptr) {
    const char *table_name = single_table->relation_name.c_str();
    if (nullptr == table_name) {
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    table_map.insert({table_name, table});
    binder_context.add_table(table);
    return RC::SUCCESS;
  }

  if (UnboundJoinedTable *joined_table = dynamic_cast<UnboundJoinedTable *>(unbound_table); joined_table != nullptr) {
    RC   rc    = RC::SUCCESS;
    auto left  = joined_table->left.get();
    auto right = joined_table->right.get();
    if (OB_FAIL(rc = collect_tables(db, left, table_map, binder_context))) {
      return rc;
    }
    if (OB_FAIL(rc = collect_tables(db, right, table_map, binder_context))) {
      return rc;
    }
    return RC::SUCCESS;
  }

  return RC::UNSUPPORTED;
}

auto SelectStmt::bind_tables(const unordered_map<string, Table *> &table_map, ExpressionBinder &expr_binder,
    UnboundTable *unbound_table) -> unique_ptr<BoundTable>
{
  if (UnboundSingleTable *single_table = dynamic_cast<UnboundSingleTable *>(unbound_table); single_table != nullptr) {
    // single_table->relation_name 一定存在
    Table *table = table_map.at(single_table->relation_name);
    return make_unique<BoundSingleTable>(table);
  }

  if (UnboundJoinedTable *joined_table = dynamic_cast<UnboundJoinedTable *>(unbound_table); joined_table != nullptr) {
    auto left  = bind_tables(table_map, expr_binder, joined_table->left.get());
    auto right = bind_tables(table_map, expr_binder, joined_table->right.get());

    if (joined_table->expr) {
      vector<unique_ptr<Expression>> bound_expressions;
      expr_binder.bind_expression(joined_table->expr, bound_expressions);
      ASSERT(bound_expressions.size() == 1, "bound_expressions' size must be 1");

      return make_unique<BoundJoinedTable>(
          joined_table->type, std::move(bound_expressions.at(0)), std::move(left), std::move(right));
    }
    return make_unique<BoundJoinedTable>(joined_table->type, nullptr, std::move(left), std::move(right));
  }

  ASSERT(false, "Unreachable code touched");
  return nullptr;
}

RC SelectStmt::collect_group_by_expressions(
    unique_ptr<GroupBy> &group_by, ExpressionBinder &binder, vector<unique_ptr<Expression>> &group_by_expressions)
{
  if (!group_by) {
    return RC::SUCCESS;
  }
  for (unique_ptr<Expression> &expression : group_by->exprs) {
    RC rc = binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
    }
  }

  if (!group_by->having_predicate) {
    return RC::SUCCESS;
  }

  auto                                   having_predicate = group_by->having_predicate->copy();
  vector<unique_ptr<Expression>>         exprs;
  function<RC(unique_ptr<Expression> &)> collector = [&](unique_ptr<Expression> &expr) -> RC {
    if (expr->type() == ExprType::AGGREGATION || expr->type() == ExprType::UNBOUND_AGGREGATION) {
      exprs.emplace_back(std::move(expr));
      return RC::SUCCESS;
    }
    return ExpressionIterator::iterate_child_expr(*expr, collector);
  };

  RC rc = ExpressionIterator::iterate_child_expr(*having_predicate, collector);
  if (OB_FAIL(rc)) {
    return rc;
  }

  for (unique_ptr<Expression> &expression : exprs) {
    rc = binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
    }
  }
  return RC::SUCCESS;
}
