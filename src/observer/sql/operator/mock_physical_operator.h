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

#include "sql/operator/physical_operator.h"

/**
 * @brief MockPhysicalOperator
 * @ingroup PhysicalOperator
 */
class MockPhysicalOperator : public PhysicalOperator 
{
public:
  MockPhysicalOperator() = default;
  virtual ~MockPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::MOCK; }
  OpType               get_op_type() const override { return OpType::PHYSICALMOCK; }

  RC open(Trx *trx) override { return RC::SUCCESS; }
  RC next() override { return RC::RECORD_EOF; }
  RC close() override { return RC::SUCCESS; }

  Tuple *current_tuple() override { return nullptr; }

private:
};
