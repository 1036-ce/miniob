/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/type/date_type.h"
#include "common/type/float_type.h"
#include "common/type/integer_type.h"
#include "common/type/vector_type.h"
#include "common/value.h"

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::hash(const Value &val, std::size_t &result) const
{
  result = std::hash<string>{}(val.get_string());
  return RC::SUCCESS;
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  RC rc;
  switch (type) {
    case AttrType::DATES: {
      rc = DateType().set_value_from_str(result, val.value_.pointer_value_);
    } break;
    case AttrType::INTS: {
      rc = IntegerType().set_value_from_str(result, val.value_.pointer_value_);
    } break;
    case AttrType::FLOATS: {
      rc = FloatType().set_value_from_str(result, val.value_.pointer_value_);
    } break;
    case AttrType::VECTORS: {
      rc = VectorType().set_value_from_str(result, val.value_.pointer_value_);
    } break;
    default: return RC::UNIMPLEMENTED;
  }
  return rc;
}

int CharType::cast_cost(AttrType type)
{
  switch (type) {
    case AttrType::CHARS: return 0;
    case AttrType::DATES: return 1;
    case AttrType::VECTORS: return 1;
    case AttrType::INTS: return 1;
    case AttrType::FLOATS: return 1;
    default: return INT32_MAX;
  }
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}
