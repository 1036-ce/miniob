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
// Created by WangYunlai on 2022/12/30.
//

#include "sql/operator/nested_loop_join_physical_operator.h"

NestedLoopJoinPhysicalOperator::NestedLoopJoinPhysicalOperator() {}

RC NestedLoopJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("nlj operator should have 2 children");
    return RC::INTERNAL;
  }

  RC rc         = RC::SUCCESS;
  left_         = children_[0].get();
  right_        = children_[1].get();
  right_closed_ = true;
  round_done_   = true;

  rc   = left_->open(trx);
  trx_ = trx;
  return rc;
}

RC NestedLoopJoinPhysicalOperator::next()
{
  RC   left_rc             = RC::SUCCESS;
  RC   right_rc             = RC::SUCCESS;
  RC rc = RC::SUCCESS;

  // first left next
  if (left_tuple_ == nullptr) {
    if (OB_FAIL(left_rc = left_->next())) {
      return left_rc;
    }
    left_tuple_ = left_->current_tuple();
    if (OB_FAIL(right_rc = right_->open(trx_))) {
      return right_rc;
    }
  }

  while (true) {
    right_rc = right_->next();
    if (right_rc != RC::SUCCESS && right_rc != RC::RECORD_EOF) {
      return right_rc;
    }

    while (right_rc == RC::RECORD_EOF) {
      if (OB_FAIL(right_rc = right_->close())) {
        return right_rc;
      }

      left_rc = left_->next();
      if (left_rc != RC::SUCCESS) { // record_eof or error
        return left_rc;
      }
      left_tuple_ = left_->current_tuple();

      if (OB_FAIL(right_rc = right_->open(trx_))) {
        return right_rc;
      }
      right_rc = right_->next();
      if (right_rc != RC::SUCCESS && right_rc != RC::RECORD_EOF) {
        return right_rc;
      }
    }
    right_tuple_ = right_->current_tuple();
    joined_tuple_.set_left(left_tuple_);
    joined_tuple_.set_right(right_tuple_);

    bool filter_result;
    rc = filter(joined_tuple_, filter_result);
    if (rc != RC::SUCCESS) {
      LOG_TRACE("Joined tuple filtered failed=%s", strrc(rc));
      return rc;
    }

    if (filter_result) {
      return RC::SUCCESS;
    }
  }
}

RC NestedLoopJoinPhysicalOperator::close()
{
  RC rc = left_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close left oper. rc=%s", strrc(rc));
  }
  return rc;
}
 
Tuple *NestedLoopJoinPhysicalOperator::current_tuple() { return &joined_tuple_; }

RC NestedLoopJoinPhysicalOperator::left_next()
{
  RC rc = RC::SUCCESS;
  rc = left_->next();
  if (rc != RC::SUCCESS) { // record_eof or error
    return rc;
  }
  left_tuple_ = left_->current_tuple();
  return rc;
}

RC NestedLoopJoinPhysicalOperator::right_next()
{
  RC rc = RC::SUCCESS;
  if (round_done_) {
    if (!right_closed_) {
      rc = right_->close();

      right_closed_ = true;
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }

    rc = right_->open(trx_);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    right_closed_ = false;

    round_done_ = false;
  }

  rc = right_->next();
  if (rc != RC::SUCCESS) {
    if (rc == RC::RECORD_EOF) {
      round_done_ = true;
    }
    return rc;
  }

  right_tuple_ = right_->current_tuple();
  joined_tuple_.set_right(right_tuple_);
  return rc;
}

RC NestedLoopJoinPhysicalOperator::filter(const JoinedTuple &tuple, bool &result)
{
  if (!join_predicate_) {
    result = true;
    return RC::SUCCESS;
  }

  RC    rc = RC::SUCCESS;
  Value value;

  rc = join_predicate_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  bool tmp_result = value.get_boolean();
  if (!tmp_result) {
    result = false;
    return rc;
  }

  result = true;
  return rc;
}
