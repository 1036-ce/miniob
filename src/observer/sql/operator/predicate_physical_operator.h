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
// Created by WangYunlai on 2022/6/27.
//

#pragma once

#include "sql/expr/expression.h"
#include "sql/expr/subquery_expression.h"
#include "sql/operator/physical_operator.h"

class FilterStmt;

/**
 * @brief 过滤/谓词物理算子
 * @ingroup PhysicalOperator
 *
 * @detail 如果Predicate有多个children, 默认最后一个为当前查询的operator，前面的所有均为子查询
 */
class PredicatePhysicalOperator : public PhysicalOperator
{
public:
  PredicatePhysicalOperator(unique_ptr<Expression> expr);

  virtual ~PredicatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::PREDICATE; }
  OpType               get_op_type() const override { return OpType::FILTER; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

  RC tuple_schema(TupleSchema &schema) const override;

  void add_subquery(SubQueryExpr* subquery) {
    subqueries_.push_back(subquery);
  }
/*   void add_subquery_child(unique_ptr<PhysicalOperator> oper) { 
 *     children_.emplace_back(std::move(oper)); 
 *     is_subquery_.push_back(1);
 *   }
 * 
 *   void add_non_subquery_child(unique_ptr<PhysicalOperator> oper) { 
 *     children_.emplace_back(std::move(oper)); 
 *     is_subquery_.push_back(0);
 *   } */

private:

  RC open_correlated_subquery();
  RC close_correlated_subquery();
  ComparisonExpr* find_subquery_parent(SubQueryExpr* subquery);

  unique_ptr<Expression> expression_;

  std::vector<SubQueryExpr*> subqueries_;


  // vector<int> is_subquery_;  // 如过children_[i]是一个子查询, is_subquey_[i]==1, 否则is_subquey_[i]==0
  // PhysicalOperator *non_subquery_child_{nullptr};
};
