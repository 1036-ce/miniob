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

#include "common/type/vector_type.h"
#include "storage/index/index.h"

struct SearchEntry {
  size_t idx;
  float distance;

  bool operator<(const SearchEntry& other) const {
    return this->distance < other.distance;
  }
};

/**
 * @brief ivfflat 向量索引
 * @ingroup Index
 */
class IvfflatIndex : public Index
{
public:
  IvfflatIndex() = default;
  virtual ~IvfflatIndex() noexcept {};

  RC create(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta,
      const unordered_map<string, string> &params) override;
  RC open(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta,
      const unordered_map<string, string> &params) override;

  bool is_vector_index() override { return true; }

  vector<RID> ann_search(const vector<float> &base_vector, int limit);

  RC close() { return RC::SUCCESS; }

  RC insert_entry(const char *record, const RID *rid) override { return RC::UNIMPLEMENTED; };
  RC insert_entry(const Record &record) override;
  RC delete_entry(const char *record, const RID *rid) override { return RC::UNIMPLEMENTED; };
  RC delete_entry(const Record &record) override;

  IndexScanner *create_scanner(
      vector<Value> left_values, bool left_inclusive, vector<Value> right_values, bool right_inclusive) override
  {
    return nullptr;
  }

  IndexScanner *create_scanner(const char *left_key, int left_len, bool left_inclusive, const char *right_key,
      int right_len, bool right_inclusive) override
  {
    return nullptr;
  }

  RC sync() override { return RC::SUCCESS; };

  RC kmeans_train();

  float get_match_score(const TableGetLogicalOperator &oper) override;

  unique_ptr<PhysicalOperator> gen_physical_oper(const TableGetLogicalOperator &oper) override;

private:
  // use kmeans++ to init
  RC  kmeans_init();
  RC  str2int(const string &str, int &val);
  RC  get_value_from_record(const Record &record, Value &val);
  RC  distance(const Value &left, const Value &right, float &result);
  RC  get_nearest_center_distance(const Value &val, float &dist, int &center_idx);
  int choose(const vector<float> &dists, float rand);
  void remove_deleted();
  bool need_retrain();

  bool           trained_ = false;
  Table         *table_   = nullptr;
  FieldMeta      field_meta_;
  VectorFuncType func_type_;
  int            lists_  = 1;
  int            probes_ = 1;

  vector<RID>   rids_;
  vector<Value> values_;

  vector<Value>          centers_;
  vector<vector<size_t>> clusters_;

  int insert_num_after_train_ = 0;
  int delete_num_after_train_ = 0;
};
