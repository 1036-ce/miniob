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
// Created by Wangyunlai on 2023/6/13.
//

#include "common/log/log.h"
#include "common/types.h"
#include "sql/stmt/create_table_stmt.h"
#include "event/sql_debug.h"


RC CreateTableStmt::create(Db *db, const CreateTableSqlNode &create_table, Stmt *&stmt)
{
  StorageFormat storage_format = get_storage_format(create_table.storage_format.c_str());
  if (storage_format == StorageFormat::UNKNOWN_FORMAT) {
    return RC::INVALID_ARGUMENT;
  }

  if (create_table.select_sql_node == nullptr) {
    stmt = new CreateTableStmt(create_table.relation_name, create_table.attr_infos, create_table.primary_keys, storage_format);
    sql_debug("create table statement: table name %s", create_table.relation_name.c_str());
    return RC::SUCCESS;
  }

  RC rc = RC::SUCCESS;
  Stmt* select_stmt = nullptr;
  if(OB_FAIL(rc = SelectStmt::create(db, *create_table.select_sql_node, select_stmt))) {
    LOG_WARN("create select stmt failed");
    return rc;
  }
  vector<AttrInfoSqlNode> attr_infos;
  if (OB_FAIL(rc = get_attr_infos(static_cast<SelectStmt*>(select_stmt), create_table.attr_infos, attr_infos))) {
    return rc;
  }
  stmt = new CreateTableStmt(create_table.relation_name, attr_infos, create_table.primary_keys, storage_format, static_cast<SelectStmt*>(select_stmt));

  // stmt = new CreateTableStmt(static_cast<SelectStmt*>(select_stmt), create_table.relation_name, create_table.primary_keys, storage_format);

  return RC::SUCCESS;
}

StorageFormat CreateTableStmt::get_storage_format(const char *format_str) {
  StorageFormat format = StorageFormat::UNKNOWN_FORMAT;
  if (strlen(format_str) == 0) {
    format = StorageFormat::ROW_FORMAT;
  } else if (0 == strcasecmp(format_str, "ROW")) {
    format = StorageFormat::ROW_FORMAT;
  } else if (0 == strcasecmp(format_str, "PAX")) {
    format = StorageFormat::PAX_FORMAT;
  } else {
    format = StorageFormat::UNKNOWN_FORMAT;
  }
  return format;
}

RC CreateTableStmt::get_attr_infos(SelectStmt* select_stmt, const vector<AttrInfoSqlNode>& attr_infos, vector<AttrInfoSqlNode>& result) {
  if (!attr_infos.empty() && attr_infos.size() != select_stmt->query_expressions().size()) {
    return RC::INVALID_ARGUMENT;
  }
  if (attr_infos.empty()) {
    for (const auto& expr: select_stmt->query_expressions()) {
      AttrInfoSqlNode attr_info{expr->value_type(), expr->name(), static_cast<size_t>(expr->value_length()), true};
      result.push_back(attr_info);
    }
  }
  else {
    const auto& query_exprs = select_stmt->query_expressions();
    for (size_t i = 0; i < query_exprs.size(); ++i) {
      const auto& expr = query_exprs.at(i);
      AttrInfoSqlNode attr_info = attr_infos.at(i);
      if (attr_info.name != expr->name() || attr_info.type != expr->value_type()) {
        return RC::INVALID_ARGUMENT;
      }
    }
    result = attr_infos;
  }
  return RC::SUCCESS;
}
