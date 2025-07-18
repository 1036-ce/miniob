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

/* RC get_table_and_field(Db *db, Table *default_table, unordered_map<string, Table *> *tables, const RelAttrSqlNode &attr,
 *     Table *&table, const FieldMeta *&field)
 * {
 *   if (common::is_blank(attr.relation_name.c_str())) {
 *     table = default_table;
 *   } else if (nullptr != tables) {
 *     auto iter = tables->find(attr.relation_name);
 *     if (iter != tables->end()) {
 *       table = iter->second;
 *     }
 *   } else {
 *     table = db->find_table(attr.relation_name.c_str());
 *   }
 *   if (nullptr == table) {
 *     LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
 *     return RC::SCHEMA_TABLE_NOT_EXIST;
 *   }
 * 
 *   field = table->table_meta().field(attr.attribute_name.c_str());
 *   if (nullptr == field) {
 *     LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
 *     table = nullptr;
 *     return RC::SCHEMA_FIELD_NOT_EXIST;
 *   }
 * 
 *   return RC::SUCCESS;
 * } */

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
