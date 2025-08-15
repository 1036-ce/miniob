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

#include "storage/index/bplus_tree_index.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/db/db.h"

BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

RC BplusTreeIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const vector<FieldMeta> &field_metas) {
   if (inited_) {
     LOG_WARN("Failed to create index due to the index has been created before. file_name:%s, index:%s",
         file_name, index_meta.name());
     return RC::RECORD_OPENNED;
   }
 
   Index::init(index_meta, field_metas);
 
   BufferPoolManager &bpm = table->db()->buffer_pool_manager();
   RC rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, field_metas);
   if (RC::SUCCESS != rc) {
     LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, rc:%s",
         file_name, index_meta.name(), strrc(rc));
     return rc;
   }
 
   inited_ = true;
   table_  = table;
   LOG_INFO("Successfully create index, file_name:%s, index:%s",
     file_name, index_meta.name());
   return RC::SUCCESS;
 
}

RC BplusTreeIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const vector<FieldMeta> &field_metas) {
   if (inited_) {
     LOG_WARN("Failed to open index due to the index has been initedd before. file_name:%s, index:%s",
         file_name, index_meta.name());
     return RC::RECORD_OPENNED;
   }
 
   Index::init(index_meta, field_metas);
 
   BufferPoolManager &bpm = table->db()->buffer_pool_manager();
   RC rc = index_handler_.open(table->db()->log_handler(), bpm, file_name);
   if (RC::SUCCESS != rc) {
     LOG_WARN("Failed to open index_handler, file_name:%s, index:%s, rc:%s",
         file_name, index_meta.name(), strrc(rc));
     return rc;
   }
 
   inited_ = true;
   table_  = table;
   LOG_INFO("Successfully open index, file_name:%s, index:%s",
     file_name, index_meta.name());
   return RC::SUCCESS;
 
}

RC BplusTreeIndex::close()
{
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s", index_meta_.name());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

RC BplusTreeIndex::insert_entry(const char *record, const RID *rid)
{
  return RC::SUCCESS;
  // return index_handler_.insert_entry(record + field_meta_.offset(), rid);
}

RC BplusTreeIndex::delete_entry(const char *record, const RID *rid)
{
  return RC::SUCCESS;
  // return index_handler_.delete_entry(record + field_meta_.offset(), rid);
}


RC BplusTreeIndex::insert_entry(const Record& record) {
  auto values = get_values(record);
  return index_handler_.insert_entry(values, &record.rid());
}

RC BplusTreeIndex::delete_entry(const Record& record) {
  auto values = get_values(record);
  return index_handler_.delete_entry(values, &record.rid());
}

IndexScanner *BplusTreeIndex::create_scanner(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  return nullptr;
  /* BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
   * RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
   * if (rc != RC::SUCCESS) {
   *   LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
   *   delete index_scanner;
   *   return nullptr;
   * }
   * return index_scanner; */
}


IndexScanner *BplusTreeIndex::create_scanner(vector<Value> left_values, bool left_inclusive, vector<Value> right_values, bool right_inclusive) {
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_values, left_inclusive, right_values, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner;
    return nullptr;
  }
  return index_scanner;
}

RC BplusTreeIndex::sync() { return index_handler_.sync(); }

int BplusTreeIndex::get_match_score(unique_ptr<Expression>& predicate, unique_ptr<Expression>& residual_predicate) {
  vector<unique_ptr<Expression> *> exprs;

  if (predicate->type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *conjunction_expr = static_cast<ConjunctionExpr *>(predicate.get());
    exprs                             = conjunction_expr->flatten(ExprType::COMPARISON);
  } else if (predicate->type() == ExprType::COMPARISON) {
    exprs.push_back(&predicate);
  } else {
    LOG_WARN("Predicate must be COMPARISON or CONJUNCTION");
    return 0;
  }

  /* auto find_equal = [](const FieldMeta& field_meta) -> unique_ptr<Expression>* { return nullptr; };
   * auto find_range = [](const FieldMeta& field_meta) -> vector<unique_ptr<Expression>*> { return {}; }; */

  /* int match_cnt = 0;
   * for (const auto& field_meta: field_metas_) {
   *   auto expr = find_equal(field_meta);
   *   if (expr == nullptr) {
   *     auto exprs = find_range(field_meta);
   *   }
   * } */

  return 0;
}

Value BplusTreeIndex::get_null_bitmap(const Record& record) {
  const TableMeta& table_meta = table_->table_meta();
  auto null_bitmap_meta = table_meta.field(NULL_BITMAP_FIELD_NAME);
  Value null_bitmap;
  null_bitmap.set_type(AttrType::BITMAP);

  if (null_bitmap_meta == nullptr) {
    null_bitmap.set_null(true);
    return null_bitmap;
  }

  null_bitmap.set_data(record.data() + null_bitmap_meta->offset(), null_bitmap_meta->len());
  return null_bitmap;
}

vector<Value> BplusTreeIndex::get_values(const Record& record) {
  Value null_bitmap_val = get_null_bitmap(record);
  common::Bitmap null_bitmap;
  if (!null_bitmap_val.is_null()) {
    null_bitmap = common::Bitmap(null_bitmap_val.get_bitmap_data(), null_bitmap_val.length());
  }

  vector<Value> values;
  for (const auto& field_meta: field_metas_) {
    Value val;
    val.set_type(field_meta.type());
    val.set_data(record.data() + field_meta.offset(), field_meta.len());
    if (!null_bitmap_val.is_null()) {
      val.set_null(null_bitmap.get_bit(field_meta.field_id()));
    }
    values.push_back(val);
  }

  return values;
}

////////////////////////////////////////////////////////////////////////////////
BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler) {}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept { tree_scanner_.close(); }

/* RC BplusTreeIndexScanner::open(
 *     const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
 * {
 *   return tree_scanner_.open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
 * } */

RC BplusTreeIndexScanner::open(vector<Value> left_values, bool left_inclusive, vector<Value> right_values, bool right_inclusive) {
  return tree_scanner_.open(left_values, left_inclusive, right_values, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid) { return tree_scanner_.next_entry(*rid); }

RC BplusTreeIndexScanner::destroy()
{
  delete this;
  return RC::SUCCESS;
}
