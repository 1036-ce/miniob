#include "storage/lob/lob_manager.h"
#include "common/config.h"
#include <cmath>

RC LobPageHandler::init(DiskBufferPool &buffer_pool, PageNum page_num, ReadWriteMode mode)
{
  if (disk_buffer_pool_ != nullptr) {
    if (frame_->page_num() == page_num) {
      LOG_WARN("Disk buffer pool has been opened for page_num %d.", page_num);
      return RC::RECORD_OPENNED;
    } else {
      cleanup();
    }
  }

  RC rc = RC::SUCCESS;
  if ((rc = buffer_pool.get_this_page(page_num, &frame_)) != RC::SUCCESS) {
    LOG_ERROR("Failed to get page handle from disk buffer pool. ret=%d:%s", rc, strrc(rc));
    return rc;
  }

  char *data = frame_->data();

  if (mode == ReadWriteMode::READ_ONLY) {
    frame_->read_latch();
  } else {
    frame_->write_latch();
  }
  disk_buffer_pool_ = &buffer_pool;

  rw_mode_         = mode;
  lob_page_header_ = (LobPageHeader *)(data);

  LOG_TRACE("Successfully init page_num %d.", page_num);
  return rc;
}

RC LobPageHandler::init_empty_page(DiskBufferPool &buffer_pool, PageNum page_num)
{
  RC rc = init(buffer_pool, page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page page_num %d. rc=%s", page_num, strrc(rc));
    return rc;
  }

  lob_page_header_->next_ = LobID{.page_id_ = BP_INVALID_PAGE_NUM};
  lob_page_header_->size_ = 0;

  return rc;
}

void LobPageHandler::write_data(const char *data, size_t size)
{
  size                    = std::min(size, LOB_PAGE_DATA_SIZE);
  lob_page_header_->size_ = size;
  char *target            = frame_->data() + LOB_PAGE_HEADER_SIZE;
  memcpy(target, data, size);
  frame_->mark_dirty();
}

void LobPageHandler::write_header(LobPageHeader lob_header) { 
  *lob_page_header_ = lob_header; 
  frame_->mark_dirty();
}

RC LobPageHandler::cleanup()
{
  if (disk_buffer_pool_ != nullptr) {
    if (rw_mode_ == ReadWriteMode::READ_ONLY) {
      frame_->read_unlatch();
    } else {
      frame_->write_unlatch();
    }
    disk_buffer_pool_->unpin_page(frame_);
    disk_buffer_pool_ = nullptr;
  }

  return RC::SUCCESS;
}

RC LobManager::init(DiskBufferPool &buffer_pool)
{
  if (disk_buffer_pool_ != nullptr) {
    LOG_ERROR("record file handler has been openned.");
    return RC::RECORD_OPENNED;
  }

  disk_buffer_pool_ = &buffer_pool;
  return RC::SUCCESS;
}

void LobManager::close()
{
  if (disk_buffer_pool_ != nullptr) {
    disk_buffer_pool_ = nullptr;
  }
}

RC LobManager::insert_lob(const char *data, int lob_size, LobID &lob_id)
{
  RC     rc = RC::SUCCESS;
  size_t offset =
      std::floor(static_cast<float>(lob_size) / static_cast<float>(LOB_PAGE_DATA_SIZE)) * LOB_PAGE_DATA_SIZE;
  size_t  size          = lob_size - offset;
  PageNum page_num      = BP_INVALID_PAGE_NUM;
  PageNum next_page_num = BP_INVALID_PAGE_NUM;

  while (true) {
    unique_ptr<LobPageHandler> lob_page_handler{new LobPageHandler};

    Frame *frame = nullptr;
    if ((rc = disk_buffer_pool_->allocate_page(&frame)) != RC::SUCCESS) {
      LOG_ERROR("Failed to allocate page while inserting lob. rc:%d", rc);
      return rc;
    }

    page_num = frame->page_num();
    rc = lob_page_handler->init_empty_page(*disk_buffer_pool_, page_num);
    if (OB_FAIL(rc)) {
      frame->unpin();
      LOG_ERROR("Failed to init empty page. rc:%d", rc);
      return rc;
    }

    // frame 在allocate_page的时候，是有一个pin的，在init_empty_page时又会增加一个，所以这里手动释放一个
    frame->unpin();

    lob_page_handler->write_data(data + offset, size);
    lob_page_handler->write_header(LobPageHeader{.next_ = {.page_id_ = next_page_num}, .size_ = size});
    lob_page_handler->cleanup();
    if (offset == 0) {
      break;
    }
    offset -= LOB_PAGE_DATA_SIZE;
    size          = LOB_PAGE_DATA_SIZE;
    next_page_num = page_num;
  }
  lob_id = LobID{.page_id_ = page_num};
  return rc;
}

RC LobManager::delete_lob(const LobID &lob_id)
{
  RC rc = RC::SUCCESS;
  LobID cur = lob_id;

  while (cur.page_id_ != BP_INVALID_PAGE_NUM) {
    unique_ptr<LobPageHandler> lob_page_handler(new LobPageHandler);
    if (OB_FAIL(rc = lob_page_handler->init(*disk_buffer_pool_, cur.page_id_, ReadWriteMode::READ_WRITE))) {
      return rc;
    }
    auto page_num = cur.page_id_;
    cur = lob_page_handler->lob_page_header()->next_;
    lob_page_handler->cleanup();
    disk_buffer_pool_->dispose_page(page_num);
  }

  return rc;
}

RC LobManager::get_lob(const LobID &lob_id, char *data, size_t& size)
{
  RC rc = RC::SUCCESS;
  size_t offset = 0;

  unique_ptr<LobPageHandler> lob_page_handler(new LobPageHandler);
  LobID cur = lob_id;

  while (cur.page_id_ != BP_INVALID_PAGE_NUM) {
    if (OB_FAIL(rc = lob_page_handler->init(*disk_buffer_pool_, cur.page_id_, ReadWriteMode::READ_ONLY))) {
      return rc;
    }
    
    memcpy(data + offset, lob_page_handler->data(), lob_page_handler->lob_page_header()->size_);

    offset += lob_page_handler->lob_page_header()->size_;
    cur = lob_page_handler->lob_page_header()->next_;
  }
  size = offset;
  return rc;
}
