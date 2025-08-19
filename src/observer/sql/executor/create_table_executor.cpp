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

#include "sql/executor/create_table_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/stmt/create_table_stmt.h"
#include "storage/db/db.h"

RC CreateTableExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  Db      *db      = session->get_current_db();
  ASSERT(stmt->type() == StmtType::CREATE_TABLE,
      "create table executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  RC rc = RC::SUCCESS;

  CreateTableStmt *create_table_stmt = static_cast<CreateTableStmt *>(stmt);
  const char      *table_name        = create_table_stmt->table_name().c_str();
  rc                                 = session->get_current_db()->create_table(table_name,
      create_table_stmt->attr_infos(),
      create_table_stmt->primary_keys(),
      create_table_stmt->storage_format());

  if (OB_FAIL(rc) || create_table_stmt->select_stmt() == nullptr) {
    return rc;
  }

  // for create-table-select statement
  Table *table = db->find_table(table_name);
  unique_ptr<PhysicalOperator> physical_oper;
  if (OB_FAIL(rc = gen_select_physical_plan(create_table_stmt->select_stmt(), physical_oper, session))) {
    return rc;
  }

  if (OB_FAIL(rc = physical_oper->open(session->current_trx()))) {
    return rc;
  }
  Tuple *tuple = nullptr;
  while (RC::SUCCESS == (rc = physical_oper->next())) {
    vector<Value> values;
    tuple = physical_oper->current_tuple();
    assert(tuple != nullptr);

    int cell_num = tuple->cell_num();
    for (int i = 0; i < cell_num; i++) {
      Value value;
      rc = tuple->cell_at(i, value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get tuple cell value. rc=%s", strrc(rc));
        physical_oper->close();
        return rc;
      }
      values.push_back(value);
    }

    Record record;
    if (OB_FAIL(rc =  table->make_record(values.size(), values.data(), record))) {
      physical_oper->close();
      return rc;
    }
    if (OB_FAIL(rc = table->insert_record(record))) {
      physical_oper->close();
      return rc;
    }
  }

  if (rc != RC::RECORD_EOF) {
    return rc;
  }

  if (OB_FAIL(rc = physical_oper->close())) {
    return rc;
  }
  return rc;
}

RC CreateTableExecutor::gen_select_physical_plan(
    SelectStmt *select_stmt, unique_ptr<PhysicalOperator> &physical_oper, Session *session)
{
  RC rc = RC::SUCCESS;

  LogicalPlanGenerator        logical_gen;
  unique_ptr<LogicalOperator> logical_operator;
  if (OB_FAIL(rc = logical_gen.create(select_stmt, logical_operator))) {
    LOG_WARN("failed to create cts select logical_operator");
    return rc;
  }

  PhysicalPlanGenerator physical_gen;
  if (OB_FAIL(rc = physical_gen.create(*logical_operator, physical_oper, session))) {
    LOG_WARN("failed to create cts select physical_operator");
    return rc;
  }
  return rc;
}
