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
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/type/char_type.h"
#include "common/value.h"

int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES, "left type is not date");
  ASSERT(right.attr_type() == AttrType::DATES, "right type is not date");
  date_t lhs = int2date(left.value_.date_value_);
  date_t rhs = int2date(right.value_.date_value_);

  LOG_DEBUG("lhs: {%d-%d-%d}, rhs: {%d-%d-%d}", lhs.year, lhs.month, lhs.day, rhs.year, rhs.month, rhs.day);

  if (lhs.year == rhs.year && lhs.month == rhs.month && lhs.day == rhs.day) {
    return 0;
  }
  bool ret = std::make_tuple(lhs.year, lhs.month, lhs.day) < std::make_tuple(rhs.year, rhs.month, rhs.day);
  if (true == ret) {
    return -1;
  }
  return 1;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
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

int DateType::cast_cost(AttrType type) {
  switch (type) {
    case AttrType::DATES: 
      return 0;
    case AttrType::CHARS:
      return 2;
    default:
      return INT32_MAX;
  }
}

RC DateType::add(const Value &left, const Value &right, Value &result) const { return RC::UNSUPPORTED; }

RC DateType::subtract(const Value &left, const Value &right, Value &result) const { return RC::UNSUPPORTED; }

RC DateType::multiply(const Value &left, const Value &right, Value &result) const { return RC::UNSUPPORTED; }

RC DateType::negative(const Value &val, Value &result) const { return RC::UNSUPPORTED; }

RC DateType::hash(const Value &val, std::size_t& result) const {
  result = std::hash<int32_t>{}(val.value_.date_value_);
  return RC::SUCCESS;
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  date_t date{.year = 0, .month = 0, .day = 0};

  const char *pos = data.c_str();
  while (*pos != '-') {
    date.year = date.year * 10 + (*pos - '0');
    ++pos;
  }
  ++pos;
  while (*pos != '-') {
    date.month = date.month * 10 + (*pos - '0');
    ++pos;
  }
  ++pos;
  while (*pos != '\0') {
    date.day = date.day * 10 + (*pos - '0');
    ++pos;
  }

  if (!valid_date(date)) {
    LOG_DEBUG("%s is not a valid date.", data.c_str());
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  int tmp                = date2int(date);
  val.value_.date_value_ = tmp;
  val.set_type(AttrType::DATES);
  return RC::SUCCESS;
}

RC DateType::to_string(const Value &val, string &result) const
{
  char   buf[12];
  date_t date = int2date(val.value_.date_value_);
  sprintf(buf, "%04d-%02d-%02d", date.year, date.month, date.day);
  result.assign(buf);
  return RC::SUCCESS;
}

DateType::date_t DateType::int2date(int val) const
{
  date_t date;
  date.year  = (val >> 16) & 0xFFFF;
  date.month = (val >> 8) & 0xFF;
  date.day   = val & 0xFF;
  return date;
}

int DateType::date2int(date_t date) const
{
  int val = 0;
  val     = (date.year << 16) | val;
  val     = (date.month << 8) | val;
  val     = date.day | val;
  return val;
}

bool DateType::valid_date(date_t date) const
{
  auto is_leapyear = [](int year) { return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0); };

  if (date.month < 1 || date.month > 12)
    return false;

  int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  // 闰年2月有29天
  if (date.month == 2 && is_leapyear(date.year)) {
    return date.day >= 1 && date.day <= 29;
  }

  return date.day >= 1 && date.day <= days[date.month - 1];
}
