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
#include "sql/expr/subquery_expression.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }
  trx_ = trx;

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  auto table_meta = table_->table_meta();
  vector<FieldMeta> visible_field_metas;
  for (int i = table_meta.unvisible_field_num(); i < table_meta.field_num(); ++i) {
    visible_field_metas.push_back(*table_meta.field(i));
  }

  if (OB_FAIL(rc = init_subqueries())) {
    return rc;
  }

  std::vector<Record> old_records;
  std::vector<Record> new_records;
  while (true) {
    rc = child->next();
    if (OB_FAIL(rc)) {
      break;
    }
    auto row_tuple = dynamic_cast<RowTuple *>(child->current_tuple());
    if (!row_tuple) {
      child->close();
      LOG_WARN("update physical operator's tuple is not RowTuple");
      return RC::INTERNAL;
    }

    // create new value list
    std::vector<Value> values;
    if (OB_FAIL(rc = get_new_values(visible_field_metas, row_tuple, values))) {
      child->close();
      return rc;
    }

    Record new_record;
    rc = table_->make_record(static_cast<int>(values.size()), values.data(), new_record);
    if (OB_FAIL(rc)) {
      child->close();
      LOG_WARN("failed to make record. rc=%s", strrc(rc));
      return rc;
    }
    new_record.set_rid(row_tuple->record().rid());

    old_records.push_back(std::move(row_tuple->record()));
    new_records.push_back(std::move(new_record)); 
  }

  if (OB_FAIL(rc) && rc != RC::RECORD_EOF) {
    child->close();
    return rc;
  }

  if (rc = child->close(); OB_FAIL(rc)) {
    return rc;
  }

  for (size_t i = 0; i < old_records.size(); ++i) {
    // if (OB_FAIL(rc = trx->update_record(table_, old_records.at(i), new_records.at(i)))) {
    if (OB_FAIL(rc = table_->update_record_with_trx(old_records.at(i), new_records.at(i), trx))) {
      return rc;
    }
  }

/*   for (Record &record : old_records) {
 *     if (rc = trx->delete_record(table_, record); OB_FAIL(rc)) {
 *       return rc;
 *     }
 *   }
 * 
 *   for (Record &record : new_records) {
 *     if (rc = trx->insert_record(table_, record); OB_FAIL(rc)) {
 *       return rc;
 *     }
 *   } */

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }

RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }

Tuple *UpdatePhysicalOperator::current_tuple() { return nullptr; }

RC UpdatePhysicalOperator::get_new_values(const vector<FieldMeta>& field_metas, RowTuple* row_tuple, vector<Value>& values)
{
  RC rc = RC::SUCCESS;

  if (OB_FAIL(rc = open_correlated_subquery(row_tuple))) {
    return rc;
  }

  values.reserve(target_expressions_.size());
  Value tmp;
  // for (const auto &expr : target_expressions_) {
  for (size_t i = 0; i < target_expressions_.size(); ++i) {
    const auto& expr = target_expressions_.at(i);
    const auto& field_meta = field_metas.at(i);
    rc = expr->get_value(*row_tuple, tmp);
    if (OB_FAIL(rc)) {
      LOG_WARN("expression get value failed");
      return rc;
    }
    if (tmp.is_null() && !field_meta.nullable()) {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    values.push_back(tmp);
  }

  if (OB_FAIL(rc = close_correlated_subquery())) {
    return rc;
  }

  return rc;
}

RC UpdatePhysicalOperator::init_subqueries()
{
  RC rc = RC::SUCCESS;
  for (const auto &expr : target_expressions_) {
    if (expr->type() != ExprType::SUBQUERY) {
      continue;
    }

    auto subquery = static_cast<SubQueryExpr *>(expr.get());
    if (!subquery->physical_oper()) {
      continue;
    }

    TupleSchema schema;
    if (OB_FAIL(rc = subquery->physical_oper()->tuple_schema(schema))) {
      return rc;
    }
    if (schema.cell_num() != 1) {
      LOG_WARN("subquery must return only one column");
      return RC::INVALID_ARGUMENT;
    }

    if (!subquery->is_correlated()) {
      if (OB_FAIL(rc = subquery->run_uncorrelated_query(trx_))) {
        return rc;
      }
    }
  }
  return rc;
}

RC UpdatePhysicalOperator::open_correlated_subquery(Tuple *env_tuple)
{
  RC rc = RC::SUCCESS;
  for (const auto &expr : target_expressions_) {
    if (expr->type() != ExprType::SUBQUERY) {
      continue;
    }

    auto subquery = static_cast<SubQueryExpr *>(expr.get());
    if (subquery->is_correlated()) {
      auto &physical_oper = subquery->physical_oper();
      physical_oper->set_env_tuple(env_tuple);
      if (OB_FAIL(rc = physical_oper->open(trx_))) {
        return rc;
      }
    }
  }
  return rc;
}

RC UpdatePhysicalOperator::close_correlated_subquery()
{
  RC rc = RC::SUCCESS;
  for (const auto &expr : target_expressions_) {
    if (expr->type() != ExprType::SUBQUERY) {
      continue;
    }

    auto subquery = static_cast<SubQueryExpr *>(expr.get());
    if (subquery->is_correlated()) {
      auto &physical_oper = subquery->physical_oper();
      physical_oper->set_env_tuple(nullptr);
      if (OB_FAIL(rc = physical_oper->close())) {
        return rc;
      }
    }
  }
  return rc;
}
