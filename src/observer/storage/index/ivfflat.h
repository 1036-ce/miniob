#pragma once

#include "common/config.h"
#include "common/lang/comparator.h"
#include "common/lang/memory.h"
#include "common/lang/sstream.h"
#include "common/lang/functional.h"
#include "common/log/log.h"
#include "common/type/vector_type.h"
#include "sql/parser/parse_defs.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/record/record_manager.h"
#include "storage/index/latch_memo.h"

static constexpr const int INVERTED_INDEX_PAGE_MAX_RID_NUM =
    (BP_PAGE_DATA_SIZE - sizeof(PageNum) - sizeof(int)) / sizeof(RID);

struct InvertedIndexPage
{
  PageNum next_;
  int     size_;
  RID     rids_[INVERTED_INDEX_PAGE_MAX_RID_NUM];
};

struct CenterEntry
{
  PageNum first_inverted_index_page_;  // 第一个存储倒排索引页的page_num
  char    vector_[];
};

struct CenterEntryRef
{
  PageNum page_;
  int     offset_;
};

static constexpr const int CENTER_REF_PAGE_MAX_ENTRY_NUM =
    (BP_PAGE_DATA_SIZE - sizeof(PageNum) - sizeof(int)) / sizeof(CenterEntryRef);

struct CenterSetPage
{
  PageNum        next_;
  int            count_;
  CenterEntryRef center_entries[CENTER_REF_PAGE_MAX_ENTRY_NUM];
};

class IvfFlatHandler
{
public:
  RC create(LogHandler &log_handler, BufferPoolManager &bpm, const char *file_name, const FieldMeta &field_meta,
      Table *table, VectorFuncType func_type, int lists, int probes);
  RC create(LogHandler &log_handler, DiskBufferPool &buffer_pool, const FieldMeta &field_meta, Table *table,
      VectorFuncType func_type, int lists, int probes);

  RC open(LogHandler &log_handler, BufferPoolManager &bpm, const char *file_name);
  RC open(LogHandler &log_handler, DiskBufferPool &buffer_pool);

private:
  RC add_center(const vector<float>& vec);
  RC add_rid_to_center(const CenterEntryRef& center_entry, const RID& rid);

  struct Header
  {
    Header() { memset(this, 0, sizeof(Header)); }
    VectorFuncType func_type_;
    int            lists_;
    int            probes_;
    int            center_entry_length_;
    PageNum        first_center_set_page_;
  };

  LogHandler     *log_handler_      = nullptr;  /// 日志处理器
  DiskBufferPool *disk_buffer_pool_ = nullptr;  /// 磁盘缓冲池
  Table          *table_            = nullptr;
  Header          header_;
};
