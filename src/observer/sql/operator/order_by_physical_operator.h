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

#include "sql/operator/physical_operator.h"
#include "sql/expr/composite_tuple.h"
#include "storage/buffer/page.h"
#include <cmath>

class SortEntry
{
public:
  SortEntry(const vector<Value> &keys, const ValueListTuple &tuple) : keys_(keys), tuple_(tuple) {}
  SortEntry(vector<Value> &&keys, ValueListTuple &&tuple) : keys_(std::move(keys)), tuple_(std::move(tuple)) {}

  SortEntry(const SortEntry &other)
  {
    this->keys_  = other.keys_;
    this->tuple_ = other.tuple_;
  }

  SortEntry(SortEntry &&other)
  {
    this->keys_  = std::move(other.keys_);
    this->tuple_ = std::move(other.tuple_);
  }

  SortEntry &operator=(const SortEntry &other)
  {
    this->keys_  = other.keys_;
    this->tuple_ = other.tuple_;
    return *this;
  }

  SortEntry &operator=(SortEntry &&other)
  {
    this->keys_  = std::move(other.keys_);
    this->tuple_ = std::move(other.tuple_);
    return *this;
  }

  void set_keys(const vector<Value> &keys) { keys_ = keys; }
  void set_tuple(const ValueListTuple &tuple) { tuple_ = tuple; }
  void set_keys(vector<Value> &&keys) { keys_ = std::move(keys); }
  void set_tuple(ValueListTuple &&tuple) { tuple_ = std::move(tuple); }

  const vector<Value>  &keys() const { return keys_; }
  const ValueListTuple &tuple() const { return tuple_; }
  vector<Value>        &keys() { return keys_; }
  ValueListTuple       &tuple() { return tuple_; }

private:
  vector<Value>  keys_;
  ValueListTuple tuple_;
};

class Comparator
{
public:
  Comparator(const vector<unique_ptr<OrderBy>> &orderbys)
  {
    for (const auto &orderby : orderbys) {
      is_asc_.push_back(orderby->is_asc);
    }
  }
  bool operator()(const SortEntry &lhs, const SortEntry &rhs)
  {
    const auto &left_key  = lhs.keys();
    const auto &right_key = rhs.keys();

    for (size_t i = 0; i < is_asc_.size(); ++i) {
      bool        is_asc    = is_asc_.at(i);
      const auto &left_val  = left_key.at(i);
      const auto &right_val = right_key.at(i);

      if (left_val.is_null()) {
        if (right_val.is_null()) {
          continue;
        }
        return is_asc;
      } else if (right_val.is_null()) {
        return !is_asc;
      }

      int res = left_val.compare(right_val);
      if (res == 0) {
        continue;
      }

      return is_asc ? res < 0 : res > 0;
    }
    return false;
  }

private:
  vector<bool> is_asc_;
};

class TopNHeap
{
public:
  TopNHeap(size_t max_size, const vector<unique_ptr<OrderBy>> &order_bys) : max_size_(max_size), comp_(order_bys)
  {
    heap_.reserve(max_size);
  }

  size_t size() const { return heap_.size(); }
  size_t max_size() const { return max_size_;}
  bool empty() const { return heap_.empty(); }

  void insert(const SortEntry& sort_entry);
  void pop();
  const SortEntry& top() const;

private:
  size_t parent(size_t index) const { return index == 0 ? 0: static_cast<size_t>(std::floor((index - 1) / 2)); }
  size_t left_child(size_t index) const { return (index * 2) + 1;}
  size_t right_child(size_t index) const { return (index * 2) + 2; }

  // float up the last element
  void float_up();

  // sink down the first element
  void sink_down();

  size_t            max_size_;
  Comparator        comp_;
  vector<SortEntry> heap_;
};

/**
 * @brief Order By
 * @ingroup PhysicalOperator
 */
class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(vector<unique_ptr<OrderBy>> orderbys, int limit = -1)
      : orderbys_(std::move(orderbys)), comp_(orderbys_), limit_(limit)
  {}

  virtual ~OrderByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }
  OpType               get_op_type() const override { return OpType::ORDERBY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  RC limit_open(Trx *trx);
  RC non_limit_open(Trx *trx);

  vector<unique_ptr<OrderBy>> orderbys_;
  /* vector<ValueListTuple>      tuples_;
   * vector<vector<Value>>       sort_keys_;
   * vector<size_t>              ids_; */

  vector<SortEntry> sort_entries_;
  Comparator        comp_;
  size_t            iter_;
  bool              first_emit_;
  int               limit_ = -1;
};
