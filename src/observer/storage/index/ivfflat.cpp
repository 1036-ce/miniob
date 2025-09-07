#include "storage/index/ivfflat.h"

static constexpr const int FIRST_INDEX_PAGE = 1;

RC IvfFlatHandler::create(LogHandler &log_handler, BufferPoolManager &bpm, const char *file_name,
    const FieldMeta &field_meta, Table *table, VectorFuncType func_type, int lists, int probes)
{
  RC rc = bpm.create_file(file_name);
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to create file. file name=%s, rc=%d:%s", file_name, rc, strrc(rc));
    return rc;
  }
  LOG_INFO("Successfully create index file:%s", file_name);

  DiskBufferPool *bp = nullptr;

  rc = bpm.open_file(log_handler, file_name, bp);
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to open file. file name=%s, rc=%d:%s", file_name, rc, strrc(rc));
    return rc;
  }
  LOG_INFO("Successfully open index file %s.", file_name);

  rc = this->create(log_handler, *bp, field_meta, table, func_type, lists, probes);
  if (OB_FAIL(rc)) {
    bpm.close_file(file_name);
    return rc;
  }

  if (field_meta.real_type() != AttrType::VECTORS) {
    LOG_WARN("can not create vector index on non-vector column");
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  LOG_INFO("Successfully create index file %s.", file_name);
  return rc;
}

RC IvfFlatHandler::create(LogHandler &log_handler, DiskBufferPool &buffer_pool, const FieldMeta &field_meta,
    Table *table, VectorFuncType func_type, int lists, int probes)
{
  RC rc = RC::SUCCESS;
  log_handler_ = &log_handler;
  disk_buffer_pool_ = &buffer_pool;

  Frame *header_frame = nullptr;
  if (OB_FAIL(rc = disk_buffer_pool_->allocate_page(&header_frame))) {
    return rc;
  }

  if (header_frame->page_num() != FIRST_INDEX_PAGE) {
    LOG_WARN("header page num should be %d but got %d. is it a new file",
              FIRST_INDEX_PAGE, header_frame->page_num());
    return RC::INTERNAL;
  }

  char *pdata = header_frame->data();
  Header *header = reinterpret_cast<Header*>(pdata);
  header->func_type_ = func_type;
  header->lists_ = lists;
  header->probes_ = probes;
  header->center_entry_length_ = field_meta.real_len() + sizeof(PageNum);
  header->first_center_set_page_ = BP_INVALID_PAGE_NUM;

  header_frame->mark_dirty();
  memcpy(&header_, pdata, sizeof(Header));

  return RC::SUCCESS;
}

RC IvfFlatHandler::open(LogHandler &log_handler, BufferPoolManager &bpm, const char *file_name) {

  if (disk_buffer_pool_ != nullptr) {
    LOG_WARN("%s has been opened before index.open.", file_name);
    return RC::RECORD_OPENNED;
  }

  DiskBufferPool *disk_buffer_pool = nullptr;

  RC rc = bpm.open_file(log_handler, file_name, disk_buffer_pool);
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to open file name=%s, rc=%d:%s", file_name, rc, strrc(rc));
    return rc;
  }

  rc = this->open(log_handler, *disk_buffer_pool);
  if (OB_SUCC(rc)) {
    LOG_INFO("open ivfflat success. filename=%s", file_name);
  }
  return rc;
}

RC IvfFlatHandler::open(LogHandler &log_handler, DiskBufferPool &buffer_pool) {
  if (disk_buffer_pool_ != nullptr) {
    LOG_WARN("ivfflat has been opened before index.open.");
    return RC::RECORD_OPENNED;
  }

  RC rc = RC::SUCCESS;

  Frame *frame = nullptr;
  rc           = buffer_pool.get_this_page(FIRST_INDEX_PAGE, &frame);
  if (OB_FAIL(rc)) {
    LOG_WARN("Failed to get first page, rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  char *pdata = frame->data();
  memcpy(&header_, pdata, sizeof(Header));
  disk_buffer_pool_ = &buffer_pool;
  log_handler_      = &log_handler;

  // close old page_handle
  buffer_pool.unpin_page(frame);

  LOG_INFO("Successfully open index");
  return RC::SUCCESS;
}

RC IvfFlatHandler::add_center(const vector<float>& vec) {
  return RC::SUCCESS;
}

RC IvfFlatHandler::add_rid_to_center(const CenterEntryRef& center_entry, const RID& rid) {
  return RC::SUCCESS;
}
