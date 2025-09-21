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

#include "common/lang/string.h"
#include <algorithm>
#include <mutex>
namespace oceanbase {

/**
 * @class ObBloomfilter
 * @brief A simple Bloom filter implementation(Need to support concurrency).
 */
class ObBloomfilter
{
public:
  /**
   * @brief Constructs a Bloom filter with specified parameters.
   *
   * @param hash_func_count Number of hash functions to use. Default is 4.
   * @param totoal_bits Total number of bits in the Bloom filter. Default is 65536.
   */
  ObBloomfilter(size_t hash_func_count = 4, size_t totoal_bits = 65536)
  {
    num_hash_                 = hash_func_count;
    capacity_                 = totoal_bits;
    std::size_t data_capacity = (capacity_ + 7) / 8;
    data_.reserve(data_capacity);
    for (std::size_t i = 0; i < data_capacity; ++i) {
      data_.push_back(0);
    }
    obj_cnt_ = 0;
  }

  /**
   * @brief Inserts an object into the Bloom filter.
   * @details This method computes hash values for the given object and sets corresponding bits in the filter.
   * @param object The object to be inserted.
   */
  void insert(const string &object)
  {
    auto indexs = hash(object.data(), object.size());
    std::scoped_lock insert_latch{latch_};
    ++obj_cnt_;
    for (std::size_t idx : indexs) {
      set(idx);
    }
  }

  /**
   * @brief Clears all entries in the Bloom filter.
   *
   * @details Resets the filter, removing all previously inserted objects.
   */
  void clear()
  {
    for (auto &val : data_) {
      val = 0;
    }
    obj_cnt_ = 0;
  }

  /**
   * @brief Checks if an object is possibly in the Bloom filter.
   *
   * @param object The object to be checked.
   * @return true if the object might be in the filter, false if definitely not.
   */
  bool contains(const string &object) const
  {
    auto indexs = hash(object.data(), object.size());

    // mqybe false positive
    return std::all_of(indexs.begin(), indexs.end(), [this](std::size_t idx) { return get(idx); });
  }

  /**
   * @brief Returns the count of objects inserted into the Bloom filter.
   */
  size_t object_count() const { return obj_cnt_; }


  /**
   * @brief Checks if the Bloom filter is empty.
   * @return true if the filter is empty, false otherwise.
   */
  bool empty() const { return 0 == object_count(); }

  void set(std::size_t idx)
  {
    std::size_t local_idx = idx / 8;
    std::size_t offset    = 7 - (idx % 8);
    uint8_t    &num       = data_.at(local_idx);

    num = (0x1 << offset) | num;
  }

  auto get(std::size_t idx) const -> bool
  {
    std::size_t local_idx = idx / 8;
    std::size_t offset    = 7 - (idx % 8);
    uint8_t     num       = data_.at(local_idx);

    return ((0x1 << offset) & num) != 0;
  }

private:
  auto hash(const char *key, const std::size_t len) const -> std::vector<std::size_t>
  {
    std::vector<std::size_t> result;
    result.reserve(num_hash_);
    for (int i = 0; i < num_hash_; ++i) {
      uint64_t val = fnv_1a(key, len, i);
      result.push_back(val % capacity_);
    }
    return result;
  }

  // FNV-1a hash (http://www.isthe.com/chongo/tech/comp/fnv/)
  static uint64_t fnv_1a(const char *key, const std::size_t len, const int seed)
  {                                                      // NOLINT
    uint64_t h = 14695981039346656037ULL + (31 * seed);  // FNV_OFFSET 64 bit with magic number seed
    for (std::size_t i = 0; i < len; ++i) {
      h = h ^ static_cast<uint8_t>(key[i]);
      h = h * 1099511628211ULL;  // FNV_PRIME 64 bit
    }
    return h;
  }

  std::mutex latch_;

  int             num_hash_{};
  size_t          capacity_{};
  size_t          obj_cnt_{};
  vector<uint8_t> data_;
};

}  // namespace oceanbase
