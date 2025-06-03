/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/hash_join_physical_operator.h"

RC HashJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("hash join operator should have 2 children");
    return RC::INTERNAL;
  }

  left_  = children_[0].get();
  right_ = children_[1].get();
  trx_   = trx;

  RC rc = RC::SUCCESS;
  if (OB_FAIL(rc = right_->open(trx_))) {
    return rc;
  }

  if (OB_FAIL(rc = build_hash_table())) {
    return rc;
  }

  begin_iter_ = ht_.end();
  end_iter_   = ht_.end();
  return rc;
}

RC HashJoinPhysicalOperator::next() {
  if (begin_iter_ != end_iter_) {
    auto& left_tuple = begin_iter_.val();
    joined_tuple_.set_left(&left_tuple);
    ++begin_iter_;
    return RC::SUCCESS;
  }

  RC rc = RC::SUCCESS;

  while (OB_SUCC(rc = right_->next())) {
    right_tuple_ = right_->current_tuple();
    joined_tuple_.set_right(right_tuple_);

    HashJoinKey right_key;
    if (OB_FAIL(rc = right_key_exprs_.at(0)->get_value(*right_tuple_, right_key.key_))) {
      return rc;
    }

    auto [beg, end] = ht_.find(right_key);
    if (beg != end) {
      auto& left_tuple = beg.val();
      joined_tuple_.set_left(&left_tuple);
      ++beg;
      begin_iter_ = beg;
      end_iter_ = end;
      return RC::SUCCESS;
    }
  }
  return rc;
}

RC HashJoinPhysicalOperator::close() { 
  RC rc = right_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close right oper. rc=%s", strrc(rc));
  }
  return rc;
}

Tuple *HashJoinPhysicalOperator::current_tuple() { return &joined_tuple_; }

auto HashJoinPhysicalOperator::build_hash_table() -> RC
{
  RC rc = RC::SUCCESS;

  if (OB_FAIL(rc = left_->open(trx_))) {
    return rc;
  }

  while (true) {
    rc = left_->next();
    if (rc != RC::SUCCESS) {
      if (rc == RC::RECORD_EOF) {
        break;
      }
      return rc;
    }

    left_tuple_ = left_->current_tuple();
    HashJoinKey left_key;
    if (OB_FAIL(rc = left_key_exprs_[0]->get_value(*left_tuple_, left_key.key_))) {
      return rc;
    }

    ValueListTuple val_list_tuple;
    if (OB_FAIL(rc = ValueListTuple::make(*left_tuple_, val_list_tuple))) {
      return rc;
    }
    ht_.insert(left_key, val_list_tuple);
  }

  if (OB_FAIL(rc = left_->close())) {
    LOG_WARN("failed to close left oper. rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}
