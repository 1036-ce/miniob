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

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"

/**
 * @brief update
 * @ingroup LogicalOperator
 * */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator() = default;
  UpdateLogicalOperator(Table *table, vector<unique_ptr<Expression>> &&target_expressions)
      : table_(table), target_expressions_(std::move(target_expressions))
  {}
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  OpType              get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table* table() const { return table_; }
  vector<unique_ptr<Expression>>& target_expressions() { return target_expressions_; }

private:
  Table                         *table_ = nullptr;
  vector<unique_ptr<Expression>> target_expressions_;
};
