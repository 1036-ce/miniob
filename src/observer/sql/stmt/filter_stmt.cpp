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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/sys/rc.h"
#include "sql/parser/expression_binder.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

FilterStmt::~FilterStmt()
{
}

RC FilterStmt::create(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
    unique_ptr<Expression> predicate, FilterStmt *&stmt)
{
  if (predicate == nullptr) {
    stmt = nullptr;
    return RC::SUCCESS;
  }

  RC            rc = RC::SUCCESS;
  BinderContext binder_context;
  binder_context.set_db(db);
  for (const auto &[_, table] : *tables) {
    binder_context.add_table(table);
  }
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);

  rc = expression_binder.bind_expression(predicate, bound_expressions);
  if (OB_FAIL(rc)) {
    LOG_INFO("bind expression failed. rc=%s", strrc(rc));
    return rc;
  }

  stmt = new FilterStmt(std::move(bound_expressions.front()));
  return RC::SUCCESS;
}

RC FilterStmt::create(Db *db, BinderContext& binder_context, unique_ptr<Expression> predicate, FilterStmt *&stmt) {
  if (predicate == nullptr) {
    stmt = nullptr;
    return RC::SUCCESS;
  }

  RC            rc = RC::SUCCESS;
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);

  rc = expression_binder.bind_expression(predicate, bound_expressions);
  if (OB_FAIL(rc)) {
    LOG_INFO("bind expression failed. rc=%s", strrc(rc));
    return rc;
  }

  stmt = new FilterStmt(std::move(bound_expressions.front()));
  return RC::SUCCESS;

}

RC FilterStmt::check(const FilterUnit &filter_unit)
{
  const FilterObj &rhs  = filter_unit.right();
  CompOp           comp = filter_unit.comp();

  if (comp == CompOp::IS || comp == CompOp::IS_NOT) {
    if (rhs.is_attr || !rhs.value.is_null()) {
      LOG_DEBUG("(is)/(is not) must follwed by null");
      return RC::SQL_SYNTAX;
    }
  }

  return RC::SUCCESS;
}
