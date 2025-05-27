/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/update_physical_operator.h"
#include "event/sql_debug.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  auto table_meta = table_->table_meta();

  std::vector<Record> old_records;
  std::vector<Record> new_records;
  while (true) {
    rc = child->next();
    if (OB_FAIL(rc)) {
      break;
    }
    auto row_tuple = dynamic_cast<RowTuple *>(child->current_tuple());
    if (!row_tuple) {
      LOG_WARN("update physical operator's tuple is not RowTuple");
      return RC::INTERNAL;
    }

    // create new value list
    std::vector<Value> values;
    values.reserve(target_expressions_.size());
    Value tmp;
    for (const auto &expr : target_expressions_) {
      rc = expr->get_value(*row_tuple, tmp);
      if (OB_FAIL(rc)) {
        LOG_WARN("expression get value failed");
        return rc;
      }
      values.push_back(tmp);
    }

    Record new_record;
    rc = table_->make_record(static_cast<int>(values.size()), values.data(), new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to make record. rc=%s", strrc(rc));
      return rc;
    }

    old_records.push_back(std::move(row_tuple->record()));
    new_records.push_back(std::move(new_record));

    // delete old record from table;
    /* rc = trx->delete_record(table_, row_tuple->record());
     * if (OB_FAIL(rc)) {
     *   LOG_WARN("failed to delete record. rc=%s", strrc(rc));
     *   return rc;
     * } */

    // insert new record into table;
    /* Record record;
     * rc = table_->make_record(static_cast<int>(values.size()), values.data(), record);
     * if (OB_FAIL(rc)) {
     *   LOG_WARN("failed to make record. rc=%s", strrc(rc));
     *   return rc;
     * }
     * rc = trx->insert_record(table_, record);
     * if (OB_FAIL(rc)) {
     *   LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
     * } */
  }

  if (OB_FAIL(rc) && rc != RC::RECORD_EOF) {
    return rc;
  }

  if (rc = child->close(); OB_FAIL(rc)) {
    return rc;
  }

  for (Record& record: old_records) {
    if (rc = trx->delete_record(table_, record); OB_FAIL(rc)) {
      return rc;
    }
  }

  for (Record& record: new_records) {
    if (rc = trx->insert_record(table_, record); OB_FAIL(rc)) {
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }

RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }

Tuple *UpdatePhysicalOperator::current_tuple() { return nullptr; }
