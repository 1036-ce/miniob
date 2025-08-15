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

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/parser/expression_binder.h"
#include "storage/db/db.h"
#include "storage/field/field.h"

/* UpdateStmt::UpdateStmt(Table *table, Value *values, int value_amount)
 *     : table_(table), values_(values), value_amount_(value_amount)
 * {} */

RC UpdateStmt::create(Db *db, UpdateSqlNode &update, Stmt *&stmt)
{
  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check whether the field exists
  const auto& assignment_list = update.assignment_list;
  const auto& table_meta = table->table_meta();
  for (const auto& assignment: assignment_list) {
    const string& attribute_name = assignment->attribute_name;
    auto field_meta = table_meta.field(attribute_name.c_str());
    if (!field_meta) {
      LOG_WARN("No such field. db=%s, table_name=%s, field_name=%s", db->name(), table_name, attribute_name.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
  }

  BinderContext binder_context;
  binder_context.add_table(table);
  vector<unique_ptr<Expression>> target_expressions;
  ExpressionBinder expression_binder(binder_context);

  for (int i = table_meta.unvisible_field_num(); i < table_meta.field_num(); ++i) {
    const FieldMeta* field_meta = table_meta.field(i);
    unique_ptr<Expression> expr(new UnboundFieldExpr(table_meta.name(), field_meta->name()));
    for (const auto& assignment: assignment_list) {
      if (strcmp(field_meta->name(), assignment->attribute_name.c_str()) == 0) {
        expr.reset(assignment->expr);
        break;
      }
    }

    RC rc = expression_binder.bind_expression(expr, target_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }

    // 如果当前字段不能为null,但是却要更新为null, 返回错误
    Value tmp;
    if (OB_SUCC(rc = target_expressions.back()->try_get_value(tmp))) {
      if (tmp.is_null() && !field_meta->nullable()) {
        LOG_WARN("Field '%s' can not be null", field_meta->name());
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
    }
  }


  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  unordered_map<string, Table *> table_map;
  table_map.insert({table_meta.name(), table});
  RC rc = FilterStmt::create(db, table, &table_map, std::move(update.condition), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  UpdateStmt* update_stmt = new UpdateStmt(table, std::move(target_expressions));
  update_stmt->filter_stmt_ = filter_stmt;

  stmt = update_stmt;
  return RC::SUCCESS; 
}
