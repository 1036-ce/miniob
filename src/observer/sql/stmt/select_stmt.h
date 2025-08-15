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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include "common/lang/unordered_map.h"
#include "common/sys/rc.h"
#include "sql/parser/expression_binder.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class Db;
class Table;

class BoundTable
{
public:
  virtual ~BoundTable() = default;
};

class BoundSingleTable : public BoundTable
{
public:
  BoundSingleTable(Table *table) : table_(table) {}

  Table *table() const { return table_; }

private:
  Table *table_;
};

class BoundJoinedTable : public BoundTable
{
public:
  BoundJoinedTable(
      JoinType type, unique_ptr<Expression> expr, unique_ptr<BoundTable> left, unique_ptr<BoundTable> right)
      : type_(type), expr_(std::move(expr)), left_(std::move(left)), right_(std::move(right))
  {}

  BoundJoinedTable(JoinType type, unique_ptr<Expression> expr, BoundTable *left, BoundTable *right)
      : type_(type), expr_(std::move(expr)), left_(left), right_(right)
  {}

  JoinType                type() const { return type_; }
  unique_ptr<Expression> &expr() { return expr_; }
  unique_ptr<BoundTable> &left() { return left_; }
  unique_ptr<BoundTable> &right() { return right_; }

private:
  JoinType               type_;
  unique_ptr<Expression> expr_;
  unique_ptr<BoundTable> left_;
  unique_ptr<BoundTable> right_;
};

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override { return StmtType::SELECT; }

public:
  static RC create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt);
  // for subquery
  static RC create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt, BinderContext& binder_context);

public:
  const unique_ptr<BoundTable> &table_tree() const { return table_tree_; }
  unique_ptr<BoundTable>       &table_tree() { return table_tree_; }
  FilterStmt                   *filter_stmt() const { return filter_stmt_; }
  FilterStmt                   *having_filter_stmt() const { return having_filter_stmt_; }

  vector<unique_ptr<Expression>> &query_expressions() { return query_expressions_; }
  vector<unique_ptr<Expression>> &group_by() { return group_by_; }
  vector<unique_ptr<OrderBy>>    &order_by() { return order_by_; }

private:
  static auto collect_tables(
      Db *db, UnboundTable *table_ref, unordered_map<string, Table *> &table_map, BinderContext &binder_context) -> RC;
  static auto bind_tables(const unordered_map<string, Table *> &table_map, ExpressionBinder &expr_binder,
      UnboundTable *unbound_table) -> unique_ptr<BoundTable>;

  static RC collect_group_by_expressions(unique_ptr<GroupBy>& group_by, ExpressionBinder& binder, vector<unique_ptr<Expression>>& group_by_expressions);

  vector<unique_ptr<Expression>> query_expressions_;
  unique_ptr<BoundTable>         table_tree_;
  FilterStmt                    *filter_stmt_ = nullptr;
  vector<unique_ptr<Expression>> group_by_;
  FilterStmt                    *having_filter_stmt_ = nullptr;
  vector<unique_ptr<OrderBy>>    order_by_;
};
