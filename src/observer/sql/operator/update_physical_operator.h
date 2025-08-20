/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/sys/rc.h"
#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"
#include "storage/record/record_scanner.h"
#include "common/types.h"

/**
 * @brief update物理算子
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, vector<unique_ptr<Expression>> &&target_expressions)
      : table_(table), target_expressions_(std::move(target_expressions))
  {}
  virtual ~UpdatePhysicalOperator() = default;

  string param() const override { return table_->name(); }

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }
  OpType               get_op_type() const override { return OpType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  RC get_new_values(RowTuple* row_tuple, vector<Value>& values);
  RC init_subqueries();
  RC open_correlated_subquery(Tuple *env_tuple);
  RC close_correlated_subquery();

  Table                         *table_ = nullptr;
  vector<unique_ptr<Expression>> target_expressions_;
};
