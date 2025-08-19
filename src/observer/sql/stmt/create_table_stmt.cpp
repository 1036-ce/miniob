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


CreateTableStmt::CreateTableStmt(SelectStmt* select_stmt, const string& table_name, const vector<string>& pks, StorageFormat storage_format) {
  table_name_ = table_name;
  primary_keys_ = pks;
  select_stmt_ = select_stmt;
  storage_format_ = storage_format;

  for (const auto& expr: select_stmt->query_expressions()) {
    AttrInfoSqlNode attr_info{expr->value_type(), expr->name(), static_cast<size_t>(expr->value_length()), true};
    attr_infos_.push_back(attr_info);
  }
}

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
  stmt = new CreateTableStmt(static_cast<SelectStmt*>(select_stmt), create_table.relation_name, create_table.primary_keys, storage_format);

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
