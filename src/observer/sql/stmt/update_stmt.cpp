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
#include "common/config.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/expr/expression_iterator.h"
#include "sql/expr/view_field_expr.h"
#include "sql/parser/expression_binder.h"
#include "storage/db/db.h"
#include "storage/field/field.h"

/* UpdateStmt::UpdateStmt(Table *table, Value *values, int value_amount)
 *     : table_(table), values_(values), value_amount_(value_amount)
 * {} */

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}


RC UpdateStmt::create(Db *db, UpdateSqlNode &update, Stmt *&stmt)
{
  const char *relation_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == relation_name) {
    LOG_WARN("invalid argument. db=%p, relation_name=%p", db, relation_name);
    return RC::INVALID_ARGUMENT;
  }

  Table *table = db->find_table(relation_name);
  if (table != nullptr) {
    return UpdateStmt::create(db, table, std::move(update.assignment_list), std::move(update.condition), stmt);
  }

  View *view = db->find_view(relation_name);
  if (view == nullptr) {
    LOG_WARN("no such relation. db=%s, relation_name=%s", db->name(), relation_name);
    return RC::INTERNAL;
  }

  string table_name;
  for (auto& assignment: update.assignment_list) {
    auto view_field_meta = view->field_meta(assignment->attribute_name);
    if (view_field_meta == nullptr) {
      LOG_WARN("no field %s in view %s", assignment->attribute_name.c_str(), view->name().c_str());
      return RC::NOTFOUND;
    }
    if (!view_field_meta->updatable()) {
      LOG_WARN("field %s is not updatable in view", view_field_meta->name().c_str(), view->name().c_str());
      return RC::INTERNAL;
    }
    assignment->attribute_name = view_field_meta->original_field_name();
    if (table_name.empty()) {
      table_name = view_field_meta->original_table_name();
    }
  }
  table = db->find_table(table_name.c_str());


  function<RC(unique_ptr<Expression> &)> view_to_table = [table, view, &view_to_table](unique_ptr<Expression> &expr) -> RC {
    if (expr->type() == ExprType::VIEW_FIELD) {
      ViewFieldExpr *view_field_expr = static_cast<ViewFieldExpr*>(expr.get());
      const ViewFieldMeta* view_field_meta = view->field_meta(view_field_expr->field_name());
      const FieldMeta* table_field_meta = table->table_meta().field(view_field_meta->original_field_name().c_str());
      TableFieldExpr* table_field_expr = new TableFieldExpr(table, table_field_meta);
      expr.reset(table_field_expr);
      return RC::SUCCESS;
    }
    return ExpressionIterator::iterate_child_expr(*expr, view_to_table);
  };

  RC rc = RC::SUCCESS;
  for (auto& assignment: update.assignment_list) {
    unique_ptr<Expression> expr(assignment->expr);
    if (OB_FAIL(rc = view_to_table(expr))) {
      return rc;
    }
    assignment->expr = expr.release();
  }
  if (OB_FAIL(rc = view_to_table(update.condition))) {
    return rc;
  }

  return UpdateStmt::create(db, table, std::move(update.assignment_list), std::move(update.condition), stmt);

/*   // check whether the table exists
 *   Table *table = db->find_table(relation_name);
 *   if (nullptr == table) {
 *     LOG_WARN("no such table. db=%s, table_name=%s", db->name(), relation_name);
 *     return RC::SCHEMA_TABLE_NOT_EXIST;
 *   }
 * 
 *   return UpdateStmt::create(db, table, std::move(update.assignment_list), std::move(update.condition), stmt); */

/*   // check whether the field exists
 *   const auto& assignment_list = update.assignment_list;
 *   const auto& table_meta = table->table_meta();
 *   for (const auto& assignment: assignment_list) {
 *     const string& attribute_name = assignment->attribute_name;
 *     auto field_meta = table_meta.field(attribute_name.c_str());
 *     if (!field_meta) {
 *       LOG_WARN("No such field. db=%s, table_name=%s, field_name=%s", db->name(), table_name, attribute_name.c_str());
 *       return RC::SCHEMA_FIELD_MISSING;
 *     }
 *   }
 * 
 *   BinderContext binder_context;
 *   binder_context.set_db(db);
 *   binder_context.add_current_data_source(table);
 *   vector<unique_ptr<Expression>> target_expressions;
 *   ExpressionBinder expression_binder(binder_context);
 * 
 *   for (int i = table_meta.unvisible_field_num(); i < table_meta.field_num(); ++i) {
 *     const FieldMeta* field_meta = table_meta.field(i);
 *     unique_ptr<Expression> expr(new UnboundFieldExpr(table_meta.name(), field_meta->name()));
 *     for (const auto& assignment: assignment_list) {
 *       if (strcmp(field_meta->name(), assignment->attribute_name.c_str()) == 0) {
 *         expr.reset(assignment->expr);
 *         break;
 *       }
 *     }
 * 
 *     RC rc = expression_binder.bind_expression(expr, target_expressions);
 *     if (OB_FAIL(rc)) {
 *       LOG_INFO("bind expression failed. rc=%s", strrc(rc));
 *       return rc;
 *     }
 * 
 *     // 如果当前字段不能为null,但是却要更新为null, 返回错误
 *     // check null and text length
 *     Value tmp;
 *     if (OB_SUCC(rc = target_expressions.back()->try_get_value(tmp))) {
 *       if (tmp.is_null() && !field_meta->nullable()) {
 *         LOG_WARN("Field '%s' can not be null", field_meta->name());
 *         return RC::SCHEMA_FIELD_TYPE_MISMATCH;
 *       }
 *       // check text length
 *       if (field_meta->type() == AttrType::TEXT && tmp.attr_type() == AttrType::CHARS) {
 *         if (tmp.length() > TEXT_MAX_SIZE) {
 *           LOG_WARN("This string is too long");
 *           return RC::INVALID_ARGUMENT;
 *         }
 *       }
 *     }
 * 
 *   }
 * 
 *   // create filter statement in `where` statement
 *   FilterStmt *filter_stmt = nullptr;
 *   unordered_map<string, Table *> table_map;
 *   table_map.insert({table_meta.name(), table});
 *   RC rc = FilterStmt::create(db, table, &table_map, std::move(update.condition), filter_stmt);
 *   if (rc != RC::SUCCESS) {
 *     LOG_WARN("cannot construct filter stmt");
 *     return rc;
 *   }
 * 
 *   UpdateStmt* update_stmt = new UpdateStmt(table, std::move(target_expressions));
 *   update_stmt->filter_stmt_ = filter_stmt;
 * 
 *   stmt = update_stmt;
 *   return RC::SUCCESS;  */
}

RC UpdateStmt::create(Db* db, Table *table, vector<unique_ptr<Assignment>> assignment_list, unique_ptr<Expression> condition, Stmt *&stmt) {

  // check whether the field exists
  const auto& table_meta = table->table_meta();
  for (const auto& assignment: assignment_list) {
    const string& attribute_name = assignment->attribute_name;
    auto field_meta = table_meta.field(attribute_name.c_str());
    if (!field_meta) {
      LOG_WARN("No such field. db=%s, table_name=%s, field_name=%s", db->name(), table->name(), attribute_name.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
  }

  BinderContext binder_context;
  binder_context.set_db(db);
  binder_context.add_current_data_source(table);
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
    // check null and text length
    Value tmp;
    if (OB_SUCC(rc = target_expressions.back()->try_get_value(tmp))) {
      if (tmp.is_null() && !field_meta->nullable()) {
        LOG_WARN("Field '%s' can not be null", field_meta->name());
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      // check text length
      if (field_meta->real_type() == AttrType::TEXT && tmp.attr_type() == AttrType::CHARS) {
        if (tmp.length() > TEXT_MAX_SIZE) {
          LOG_WARN("This string is too long");
          return RC::INVALID_ARGUMENT;
        }
      }
    }

  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  unordered_map<string, Table *> table_map;
  table_map.insert({table_meta.name(), table});
  RC rc = FilterStmt::create(db, table, &table_map, std::move(condition), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  UpdateStmt* update_stmt = new UpdateStmt(table, std::move(target_expressions));
  update_stmt->filter_stmt_ = filter_stmt;

  stmt = update_stmt;
  return RC::SUCCESS; 
}
