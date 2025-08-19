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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAMES("field_names");
const static Json::StaticString IS_UNIQUE("is_unique");

RC IndexMeta::init(const char *name, const vector<FieldMeta> &fields, bool is_unique) {
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_  = name;
  for (auto& field: fields) {
    fields_.push_back(field.name());
  }
  is_unique_ = is_unique;

  return RC::SUCCESS;
}

RC IndexMeta::init(const char *name, const vector<string> &fields, bool is_unique) {
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_  = name;
  fields_ = fields;
  is_unique_ = is_unique;

  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME]       = name_;
  Json::Value fields_value;
  for (const auto& field: fields_) {
    Json::Value field_value = field;
    fields_value.append(field_value);
  }
  json_value[FIELD_FIELD_NAMES] = std::move(fields_value);
  json_value[IS_UNIQUE] = is_unique_;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value  = json_value[FIELD_NAME];
  const Json::Value &fields_value = json_value[FIELD_FIELD_NAMES];
  const Json::Value &is_unique_value = json_value[IS_UNIQUE];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!fields_value.isArray() || fields_value.size() <= 0) {
    LOG_ERROR("Invalid index meta. fields is not array, json value=%s", fields_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!is_unique_value.isBool()) {
    LOG_ERROR("Index is_unique is not a boolean. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  int field_num = fields_value.size();
  vector<string> fields(field_num);
  for (int i = 0; i < field_num; i++) {
    string &field = fields[i];

    const Json::Value &field_value = fields_value[i];
    field = field_value.asString();
  }


  return index.init(name_value.asCString(), fields, is_unique_value.asBool());
}

void IndexMeta::desc(ostream &os) const { 
  os << "index name=" << name_ << ", fields=("; 
  for (int i = 0; i < fields_.size(); ++i) {
    const string& field = fields_.at(i);
    os << field;
    if (i < fields_.size() - 1) {
      os << ", ";
    }
  }
  os << ")";
}
