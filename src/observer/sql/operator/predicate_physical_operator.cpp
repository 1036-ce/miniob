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

#include "sql/operator/predicate_physical_operator.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/field/field.h"
#include "storage/record/record.h"

PredicatePhysicalOperator::PredicatePhysicalOperator(std::unique_ptr<Expression> expr) : expression_(std::move(expr))
{
  ASSERT(expression_->value_type() == AttrType::BOOLEANS, "predicate's expression should be BOOLEAN type");
}

RC PredicatePhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "predicate_physical_operator's children must have one child");
  reset();
  trx_  = trx;
  RC rc = RC::SUCCESS;
  if (OB_FAIL(rc = children_.front()->open(trx))) {
    return rc;
  }

  for (auto subquery : subqueries_) {
    if (subquery->physical_oper()) {
      should_pre_execute_ = true;
      break;
    }
  }

  /* if (OB_FAIL(rc = init_subqueries(trx))) {
   *   return rc;
   * } */
  if (should_pre_execute_) {
    if (OB_FAIL(rc = init_subqueries())) {
      return rc;
    }
    if (OB_FAIL(rc = pre_execute())) {
      return rc;
    }
    iter_       = 0;
    first_emit_ = true;
    children_.front()->close();
  }
  return rc;
}

RC PredicatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (should_pre_execute_) {
    if (first_emit_) {
      first_emit_ = false;
      if (iter_ == tuples_.size()) {
        return RC::RECORD_EOF;
      }
      return RC::SUCCESS;
    }

    ++iter_;
    if (iter_ == tuples_.size()) {
      return RC::RECORD_EOF;
    }
    return RC::SUCCESS;
  } else {
    PhysicalOperator *non_subquery_child = children_.front().get();
    while (OB_SUCC(rc = non_subquery_child->next())) {
      Tuple *tuple = non_subquery_child->current_tuple();
      if (nullptr == tuple) {
        LOG_WARN("failed to get tuple from operator");
        return RC::INTERNAL;
      }

      Value value;
      if (OB_FAIL(rc = expression_->get_value(*tuple, value))) {
        return rc;
      }
      if (value.get_boolean()) {
        return rc;
      }
    }
    return rc;
  }
  /*   RC                rc                 = RC::SUCCESS;
   *   PhysicalOperator *non_subquery_child = children_.front().get();
   *
   *   while (RC::SUCCESS == (rc = non_subquery_child->next())) {
   *     Tuple *tuple = non_subquery_child->current_tuple();
   *     if (nullptr == tuple) {
   *       rc = RC::INTERNAL;
   *       LOG_WARN("failed to get tuple from operator");
   *       break;
   *     }
   *
   *     Value value;
   *     if (OB_FAIL(rc = open_correlated_subquery())) {
   *       return rc;
   *     }
   *     if (OB_FAIL(rc = expression_->get_value(*tuple, value))) {
   *       return rc;
   *     }
   *     if (OB_FAIL(close_correlated_subquery())) {
   *       return rc;
   *     }
   *
   *     if (value.get_boolean()) {
   *       return rc;
   *     }
   *   }
   *   return rc; */
}

RC PredicatePhysicalOperator::init_subqueries()
{
  RC rc = RC::SUCCESS;
  for (auto subquery : subqueries_) {

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
      auto cmp_expr = find_subquery_parent(subquery);
      // for '=', '>', '<', '>=', '<=', '!=', subquery must return one line
      if (cmp_expr->comp() != CompOp::IN && cmp_expr->comp() != CompOp::NOT_IN && subquery->values().size() > 1) {
        return RC::UNSUPPORTED;
      }
    }
  }
  return rc;
}

RC PredicatePhysicalOperator::pre_execute()
{
  RC                rc                 = RC::SUCCESS;
  PhysicalOperator *non_subquery_child = children_.front().get();

  while (OB_SUCC(rc = next_aux())) {
    Tuple *tuple = non_subquery_child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get tuple from operator");
      return RC::INTERNAL;
    }

    ValueListTuple valuelist_tuple;
    if (OB_FAIL(rc = ValueListTuple::make(*tuple, valuelist_tuple))) {
      return rc;
    }
    tuples_.push_back(valuelist_tuple);
  }
  /* for (auto& tuple: tuples_) {
   *   LOG_WARN("%s", tuple.to_string().c_str());
   * } */
  return rc == RC::RECORD_EOF ? RC::SUCCESS : rc;
}

RC PredicatePhysicalOperator::next_aux()
{
  RC                rc                 = RC::SUCCESS;
  PhysicalOperator *non_subquery_child = children_.front().get();

  while (RC::SUCCESS == (rc = non_subquery_child->next())) {
    Tuple *tuple = non_subquery_child->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }

    Value value;
    if (OB_FAIL(rc = open_correlated_subquery(tuple))) {
      return rc;
    }
    if (OB_FAIL(rc = expression_->get_value(*tuple, value))) {
      return rc;
    }
    if (OB_FAIL(close_correlated_subquery())) {
      return rc;
    }

    if (value.get_boolean()) {
      return rc;
    }
  }
  return rc;
}

void PredicatePhysicalOperator::reset() {
  should_pre_execute_ = false;
  tuples_.clear();
  iter_ = 0;
  first_emit_ = true;
}

RC PredicatePhysicalOperator::close()
{
  if (!should_pre_execute_) {
    return children_.front()->close();
  }
  return RC::SUCCESS;
}

Tuple *PredicatePhysicalOperator::current_tuple()
{
  if (should_pre_execute_) {
    return &tuples_.at(iter_);
  } else {
    return children_.front()->current_tuple();
  }
}

RC PredicatePhysicalOperator::tuple_schema(TupleSchema &schema) const { return children_.back()->tuple_schema(schema); }

RC PredicatePhysicalOperator::open_correlated_subquery(Tuple *env_tuple)
{
  RC rc = RC::SUCCESS;
  for (auto subquery : subqueries_) {
    if (subquery->is_correlated()) {
      auto &physical_oper = subquery->physical_oper();
      // physical_oper->set_env_tuple(current_tuple());
      physical_oper->set_env_tuple(env_tuple);
      if (OB_FAIL(rc = physical_oper->open(trx_))) {
        return rc;
      }
    }
  }
  return rc;
}

RC PredicatePhysicalOperator::close_correlated_subquery()
{
  RC rc = RC::SUCCESS;
  for (auto subquery : subqueries_) {
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

ComparisonExpr *PredicatePhysicalOperator::find_subquery_parent(SubQueryExpr *subquery)
{
  queue<Expression *> expr_que;
  expr_que.push(expression_.get());

  while (!expr_que.empty()) {

    Expression *cur = expr_que.front();
    expr_que.pop();

    if (cur->type() == ExprType::COMPARISON) {
      ComparisonExpr *cmp_expr = static_cast<ComparisonExpr *>(cur);
      if (cmp_expr->left().get() == subquery || cmp_expr->right().get() == subquery) {
        return cmp_expr;
      }
    }

    if (cur->type() == ExprType::CONJUNCTION) {
      ConjunctionExpr *conj_expr = static_cast<ConjunctionExpr *>(cur);
      expr_que.push(conj_expr->left().get());
      expr_que.push(conj_expr->right().get());
    }
  }
  return nullptr;
}
