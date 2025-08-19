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
// Created by wangyunlai.wyl on 2021/5/19.
//

#pragma once

#include "storage/index/bplus_tree.h"
#include "storage/index/index.h"
#include "sql/expr/expression.h"

/**
 * @brief B+树索引
 * @ingroup Index
 */
class BplusTreeIndex : public Index
{
public:
  BplusTreeIndex() = default;
  ~BplusTreeIndex() noexcept override;

  RC create(Table *table, const char *file_name, const IndexMeta &index_meta, const vector<FieldMeta> &field_metas, bool is_unique) override;
  RC open(Table *table, const char *file_name, const IndexMeta &index_meta, const vector<FieldMeta> &field_metas, bool is_unique) override; 
  RC close();

  RC insert_entry(const char *record, const RID *rid) override;
  RC delete_entry(const char *record, const RID *rid) override;

  RC insert_entry(const Record& record) override;
  RC delete_entry(const Record& record) override;
  
  /**
   * 扫描指定范围的数据
   */
  IndexScanner *create_scanner(const char *left_key, int left_len, bool left_inclusive, const char *right_key,
      int right_len, bool right_inclusive) override;

  IndexScanner *create_scanner(vector<Value> left_values, bool left_inclusive, vector<Value> right_values, bool right_inclusive) override;

  int get_match_score(unique_ptr<Expression>& predicate, unique_ptr<Expression>& residual_predicate) override;

  RC sync() override;

private:
  vector<Value> get_values(const Record& record);
  Value get_null_bitmap(const Record& record);

  bool             inited_ = false;
  Table           *table_  = nullptr;
  BplusTreeHandler index_handler_;
};

/**
 * @brief B+树索引扫描器
 * @ingroup Index
 */
class BplusTreeIndexScanner : public IndexScanner
{
public:
  BplusTreeIndexScanner(BplusTreeHandler &tree_handle);
  ~BplusTreeIndexScanner() noexcept override;

  RC next_entry(RID *rid) override;
  RC destroy() override;

  /* RC open(const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len,
   *     bool right_inclusive); */

  RC open(vector<Value> left_values, bool left_inclusive, vector<Value> right_values, bool right_inclusive);

private:
  BplusTreeScanner tree_scanner_;
};
