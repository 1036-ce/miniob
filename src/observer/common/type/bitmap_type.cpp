/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/bitmap.h"
#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/bitmap_type.h"
#include "common/type/char_type.h"
#include "common/value.h"

RC BitmapType::cast_to(const Value &val, AttrType type, Value &result) const
{
  RC rc;
  switch (type) {
    case AttrType::CHARS: {
      rc = CharType().set_value_from_str(result, val.to_string());
    } break;
    default: LOG_WARN("unsupported type %d", type); return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  return rc;
}

RC BitmapType::set_value_from_str(Value &val, const string &data) const
{
  int len = (data.size() >> 3) + 1;
  char *buf = new char[len];
  common::Bitmap bitmap(buf, len);
  bitmap.clear_all();

  string str = data;
  std::reverse(str.begin(), str.end());

  for (size_t i = 0; i < str.size(); ++i) {
    if (str.at(i) == '0') {
      continue;
    }
    else if (str.at(i) == '1') {
      bitmap.set_bit(i);
    }
    else {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }
  val.set_bitmap(buf, len);
  delete[] buf;
  return RC::SUCCESS;
}

RC BitmapType::to_string(const Value &val, string &result) const {
  char *buf = val.value_.pointer_value_;
  int len = val.length_;
  common::Bitmap bitmap(buf, len);
  for (int i = 0; i < len * 8; ++i) {
    if (bitmap.get_bit(i)) {
      result.push_back('1');
    }
    else {
      result.push_back('0');
    }
  }
  return RC::SUCCESS;
}
