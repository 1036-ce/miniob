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

#include "storage/buffer/disk_buffer_pool.h"

/* struct LobID
 * {
 *   PageNum page_id_;
 * }; */
using LobID = PageNum;

struct LobPageHeader
{
  LobID  next_;  // 是否需要下一页
  size_t size_;  // 在此页中的数据量
};

static constexpr size_t LOB_PAGE_HEADER_SIZE = sizeof(LobPageHeader);
static constexpr size_t LOB_PAGE_DATA_SIZE   = BP_PAGE_DATA_SIZE - sizeof(LobPageHeader);

class LobPageHandler
{
public:
  LobPageHandler() = default;
  ~LobPageHandler() { cleanup(); }

  /**
   * @brief 初始化
   *
   * @param buffer_pool 关联某个文件时，都通过buffer pool来做读写文件
   * @param page_num    当前处理哪个页面
   * @param mode        是否只读。在访问页面时，需要对页面加锁
   */
  RC init(DiskBufferPool &buffer_pool, PageNum page_num, ReadWriteMode mode);

  /**
   * @brief 对一个新的页面做初始化，初始化关于该页面记录信息的页头PageHeader
   *
   * @param buffer_pool 关联某个文件时，都通过buffer pool来做读写文件
   * @param page_num    当前处理哪个页面
   */
  RC init_empty_page(DiskBufferPool &buffer_pool, PageNum page_num);

  /**
   * @brief 向一个页面写size bytes的数据
   */
  void write_data(const char *data, size_t size);

  void write_header(LobPageHeader lob_header);

  /**
   * @brief 操作结束后做的清理工作，比如释放页面、解锁
   */
  RC cleanup();

  /**
   * @brief 返回该记录页的页号
   */
  PageNum get_page_num() const { return frame_->page_num(); }

  LobPageHeader       *lob_page_header() { return lob_page_header_; }
  const LobPageHeader *lob_page_header() const { return lob_page_header_; }

  char       *data() { return frame_->data() + LOB_PAGE_HEADER_SIZE; };
  const char *data() const { return frame_->data() + LOB_PAGE_HEADER_SIZE; };

private:
  DiskBufferPool *disk_buffer_pool_ = nullptr;
  Frame          *frame_            = nullptr;
  ReadWriteMode   rw_mode_          = ReadWriteMode::READ_WRITE;  ///< 当前的操作是否都是只读的
  LobPageHeader  *lob_page_header_  = nullptr;                    ///< 当前页面上页面头
};

class LobManager
{
public:
  LobManager()  = default;
  ~LobManager() = default;

  RC init(DiskBufferPool &buffer_pool);

  void close();

  RC insert_lob(const char *data, int lob_size, LobID &lob_id);

  RC delete_lob(const LobID &lob_id);

  // The data must big enough to store the lob
  RC get_lob(const LobID &lob_id, char *data, size_t &size);

private:
  DiskBufferPool        *disk_buffer_pool_ = nullptr;                    ///< 当前操作的buffer pool(文件)
  ReadWriteMode          rw_mode_          = ReadWriteMode::READ_WRITE;  ///< 当前的操作是否都是只读的
};
