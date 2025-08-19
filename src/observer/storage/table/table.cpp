/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/13.
//

#include <limits.h>
#include <string.h>

#include "common/config.h"
#include "common/defs.h"
#include "common/lang/string.h"
#include "common/lang/span.h"
#include "common/lang/algorithm.h"
#include "common/log/log.h"
#include "common/global_context.h"
#include "storage/db/db.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/index/index.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/record/heap_record_scanner.h"
#include "storage/record/lsm_record_scanner.h"
#include "storage/table/heap_table_engine.h"
#include "storage/table/lsm_table_engine.h"

Table::~Table() {}

RC Table::create(Db *db, int32_t table_id, const char *path, const char *name, const char *base_dir,
    span<const AttrInfoSqlNode> attributes, const vector<string> &primary_keys, StorageFormat storage_format,
    StorageEngine storage_engine)
{
  if (table_id < 0) {
    LOG_WARN("invalid table id. table_id=%d, table_name=%s", table_id, name);
    return RC::INVALID_ARGUMENT;
  }

  if (common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to create table %s:%s", base_dir, name);

  if (attributes.size() == 0) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d", name, attributes.size());
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  // 使用 table_name.table记录一个表的元数据
  // 判断表文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN;
  }

  close(fd);

  // if has lob type, then create a lob file
  string lob_file{};
  if (has_lob_type(attributes)) {
    lob_file = table_lob_file(base_dir, name);
    BufferPoolManager &bpm = db->buffer_pool_manager();
    rc = bpm.create_file(lob_file.c_str());
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to create disk buffer pool of lob file. file name=%s", lob_file.c_str());
      return rc;
    }
  }

  // 创建文件
  const vector<FieldMeta> *trx_fields = db->trx_kit().trx_fields();
  if ((rc = table_meta_.init(table_id, name, trx_fields, attributes, primary_keys, storage_format, storage_engine)) !=
      RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc;  // delete table file
  }
  if (OB_FAIL(rc = table_meta_.set_lob_file(lob_file))) {
    return rc;
  }

  fstream fs;
  fs.open(path, ios_base::out | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }

  // 记录元数据到文件中
  table_meta_.serialize(fs);
  fs.close();

  db_ = db;

  string             data_file = table_data_file(base_dir, name);
  BufferPoolManager &bpm       = db->buffer_pool_manager();
  rc                           = bpm.create_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  // rc = init_record_handler(base_dir);
  // if (rc != RC::SUCCESS) {
  //   LOG_ERROR("Failed to create table %s due to init record handler failed.", data_file.c_str());
  //   // don't need to remove the data_file
  //   return rc;
  // }

  if (table_meta_.storage_engine() == StorageEngine::HEAP) {
    engine_ = make_unique<HeapTableEngine>(&table_meta_, db_, this);
  } else if (table_meta_.storage_engine() == StorageEngine::LSM) {
    engine_ = make_unique<LsmTableEngine>(&table_meta_, db_, this);
  } else {
    rc = RC::UNSUPPORTED;
    LOG_WARN("Unsupported storage engine type: %d", table_meta_.storage_engine());
    return rc;
  }
  rc = engine_->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("Failed to open table %s due to engine open failed.", data_file.c_str());
    return rc;
  }

  LOG_INFO("Successfully create table %s:%s", base_dir, name);
  return rc;
}

RC Table::open(Db *db, const char *meta_file, const char *base_dir)
{
  // 加载元数据文件
  fstream fs;
  string  meta_file_path = string(base_dir) + common::FILE_PATH_SPLIT_STR + meta_file;
  fs.open(meta_file_path, ios_base::in | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file_path.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file_path.c_str());
    fs.close();
    return RC::INTERNAL;
  }
  fs.close();

  db_ = db;

  // // 加载数据文件
  // RC rc = init_record_handler(base_dir);
  // if (rc != RC::SUCCESS) {
  //   LOG_ERROR("Failed to open table %s due to init record handler failed.", base_dir);
  //   // don't need to remove the data_file
  //   return rc;
  // }
  RC rc = RC::SUCCESS;

  if (table_meta_.storage_engine() == StorageEngine::HEAP) {
    engine_ = make_unique<HeapTableEngine>(&table_meta_, db_, this);
  } else if (table_meta_.storage_engine() == StorageEngine::LSM) {
    engine_ = make_unique<LsmTableEngine>(&table_meta_, db_, this);
  } else {
    rc = RC::UNSUPPORTED;
    LOG_ERROR("Unsupported storage engine type: %d", table_meta_.storage_engine());
    return rc;
  }

  rc = engine_->open();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open table %s due to engine open failed.", base_dir);
    return rc;
  }

  return rc;
}

RC Table::insert_record(Record &record) { return engine_->insert_record(record); }

RC Table::visit_record(const RID &rid, function<bool(Record &)> visitor) { return engine_->visit_record(rid, visitor); }

RC Table::insert_record_with_trx(Record &record, Trx *trx) { return engine_->insert_record_with_trx(record, trx); }
RC Table::delete_record_with_trx(const Record &record, Trx *trx)
{
  return engine_->delete_record_with_trx(record, trx);
}

RC Table::update_record_with_trx(const Record &old_record, const Record &new_record, Trx *trx)
{
  return engine_->update_record_with_trx(old_record, new_record, trx);
}

RC Table::get_record(const RID &rid, Record &record) { return engine_->get_record(rid, record); }

RC Table::get_lob(const LobID &lob_id, char *data, size_t& size) const {
  return engine_->get_lob(lob_id, data, size);
}

const char *Table::name() const { return table_meta_.name(); }

const TableMeta &Table::table_meta() const { return table_meta_; }

RC Table::make_record(int value_num, const Value *values, Record &record)
{
  return engine_->make_record(value_num, values, record);
}

bool Table::has_lob_type(span<const AttrInfoSqlNode> attributes) {
  for (auto attr: attributes) {
    if (is_lob_type(attr.type)) {
      return true;
    }
  }
  return false;
}

RC Table::get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode)
{
  return engine_->get_record_scanner(scanner, trx, mode);
}

RC Table::get_chunk_scanner(ChunkFileScanner &scanner, Trx *trx, ReadWriteMode mode)
{
  return engine_->get_chunk_scanner(scanner, trx, mode);
}

RC Table::create_index(Trx *trx, const FieldMeta *field_meta, const char *index_name)
{
  return engine_->create_index(trx, field_meta, index_name);
}

RC Table::create_index(Trx *trx, const vector<FieldMeta>& field_metas, const char *index_name) {
  return engine_->create_index(trx, field_metas, index_name);
}

RC Table::create_index(Trx *trx, const vector<FieldMeta>& field_metas, const char *index_name, bool is_unique) {
  return engine_->create_index(trx, field_metas, index_name, is_unique);
}

RC Table::delete_record(const Record &record) { return engine_->delete_record(record); }

Index *Table::find_index(const char *index_name) const { return engine_->find_index(index_name); }
Index *Table::find_index_by_field(const char *field_name) const { return engine_->find_index_by_field(field_name); }

Index *Table::find_best_match_index(const unique_ptr<Expression>& predicate, unique_ptr<Expression>& residual_predicate) const {
  return nullptr;
}

RC Table::sync() { return engine_->sync(); }
