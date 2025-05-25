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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

bool aggregate_null_handle(Value& lhs, const Value& rhs) {
  if (lhs.is_null()) {
    if (!rhs.is_null()) {
      lhs = rhs;
    }
    return true;
  }
  else {
    if (rhs.is_null()) {
      return true;
    }
  }
  return false;
}

RC CountAggregator::accumulate(const Value &value) {
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  ++cnt_;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value &result) {
  result.set_int(cnt_);
  return RC::SUCCESS;
}

RC SumAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (aggregate_null_handle(value_, value)) {
    return RC::SUCCESS;
  }

  // value_ != null && value != null;
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  return Value::add(value, value_, value_);
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value) {
  cnt_ = cnt_ + !value.is_null();
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (aggregate_null_handle(value_, value)) {
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));
  
  return Value::add(value, value_, value_);
}

RC AvgAggregator::evaluate(Value &result) {
  if (value_.is_null()) {
    result = value_;
  }
  result.set_type(value_.attr_type());
  return Value::divide(value_, Value(cnt_), result);
}

RC MaxAggregator::accumulate(const Value &value) {
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (aggregate_null_handle(value_, value)) {
    return RC::SUCCESS;
  }

  // value_ != null && value != null;
  int result = value_.compare(value);
  if (result < 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result) {
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value) {
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (aggregate_null_handle(value_, value)) {
    return RC::SUCCESS;
  }

  // value_ != null && value != null;
  int result = value_.compare(value);
  if (result > 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value &result) {
  result = value_;
  return RC::SUCCESS;
}

