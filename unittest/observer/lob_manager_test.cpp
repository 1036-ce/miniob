/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <filesystem>

#include "gtest/gtest.h"
#include "storage/buffer/double_write_buffer.h"
#include "storage/clog/vacuous_log_handler.h"
#include "storage/lob/lob_manager.h"

using namespace std;
using namespace common;

static const filesystem::path directory("lob_mgr");
static const filesystem::path buffer_pool_filename = directory / "lob_mgr.bp";
BufferPoolManager             buffer_pool_manager;
VacuousLogHandler             log_handler;
static DiskBufferPool        *disk_buffer_pool = nullptr;

void test_init()
{
  filesystem::remove_all(directory);
  filesystem::create_directories(directory);

  ASSERT_EQ(RC::SUCCESS, buffer_pool_manager.init(make_unique<VacuousDoubleWriteBuffer>()));

  ASSERT_EQ(RC::SUCCESS, buffer_pool_manager.create_file(buffer_pool_filename.c_str()));
  ASSERT_EQ(RC::SUCCESS, buffer_pool_manager.open_file(log_handler, buffer_pool_filename.c_str(), disk_buffer_pool));
  ASSERT_NE(disk_buffer_pool, nullptr);
}

void test_clean()
{
  filesystem::remove_all(directory);
  ASSERT_EQ(buffer_pool_manager.close_file(buffer_pool_filename.c_str()), RC::SUCCESS);
}

std::string make_test_str(size_t size) { return std::string(size, 'a'); }

TEST(LobManagerTest, insert_in_one_page_test)
{
  test_init();

  LobManager lob_mgr;
  lob_mgr.init(*disk_buffer_pool);

  auto insert = [&](size_t size) {
    auto  str = make_test_str(size);
    LobID lob_id;
    ASSERT_EQ(RC::SUCCESS, lob_mgr.insert_lob(str.data(), str.size(), lob_id));

    unique_ptr<char[]> data(new char[size]);
    size_t             sz{};
    ASSERT_EQ(RC::SUCCESS, lob_mgr.get_lob(lob_id, data.get(), sz));

    ASSERT_EQ(sz, str.size());
    for (size_t i = 0; i < sz; ++i) {
      ASSERT_EQ(str.at(i), data[i]);
    }
  };

  vector<size_t> sizes{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 5093, 5673, 7423, 8000};

  for (auto size: sizes) {
    insert(size);
  }  
  test_clean();
}

TEST(LobManagerTest, insert_multi_page_test)
{
  test_init();

  LobManager lob_mgr;
  lob_mgr.init(*disk_buffer_pool);

  auto insert = [&](size_t size) {
    auto  str = make_test_str(size);
    LobID lob_id;
    ASSERT_EQ(RC::SUCCESS, lob_mgr.insert_lob(str.data(), str.size(), lob_id));

    unique_ptr<char[]> data(new char[size]);
    size_t             sz{};
    ASSERT_EQ(RC::SUCCESS, lob_mgr.get_lob(lob_id, data.get(), sz));

    ASSERT_EQ(sz, str.size());
    for (size_t i = 0; i < sz; ++i) {
      ASSERT_EQ(str.at(i), data[i]);
    }
  };

  vector<size_t> sizes{8192, 16384, 32768, 65536, 131072, 156732, 189234, 986723};

  for (auto size: sizes) {
    insert(size);
  } 
  test_clean();
}

TEST(LobManagerTest, insert_delete_test) {
  test_init();

  LobManager lob_mgr;
  lob_mgr.init(*disk_buffer_pool);

  vector<LobID> lob_ids;

  auto insert = [&](size_t size) -> void {
    auto  str = make_test_str(size);
    LobID lob_id;
    ASSERT_EQ(RC::SUCCESS, lob_mgr.insert_lob(str.data(), str.size(), lob_id));

    unique_ptr<char[]> data(new char[size]);
    size_t             sz{};
    ASSERT_EQ(RC::SUCCESS, lob_mgr.get_lob(lob_id, data.get(), sz));

    ASSERT_EQ(sz, str.size());
    for (size_t i = 0; i < sz; ++i) {
      ASSERT_EQ(str.at(i), data[i]);
    }
    lob_ids.push_back(lob_id);
  };

  auto remove = [&](LobID lob_id) -> void {
    ASSERT_EQ(RC::SUCCESS, lob_mgr.delete_lob(lob_id));
  };  

  vector<size_t> sizes{8192, 16384, 32768, 65536, 131072, 156732, 189234, 986723};
  insert(sizes[0]);
  insert(sizes[1]);
  insert(sizes[2]);
  insert(sizes[3]);
  remove(lob_ids[0]);
  remove(lob_ids[2]);
  insert(sizes[4]);  
  remove(lob_ids[1]);
  insert(sizes[5]);
  remove(lob_ids[3]);
  remove(lob_ids[5]);  
  insert(sizes[6]);
  insert(sizes[7]);
  remove(lob_ids[7]);
  remove(lob_ids[6]);      

  test_clean();
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
