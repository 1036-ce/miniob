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

#include "common/lang/unordered_map.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"

struct HashJoinKey
{
  // TODO: ValueListtuple may be better
  Value key_;

  auto operator==(const HashJoinKey &other) const -> bool { return key_ == other.key_; }
};

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<HashJoinKey>
{
  auto operator()(const HashJoinKey &key) const -> std::size_t
  {
    std::size_t ret;
    RC          rc = Value::hash(key.key_, ret);
    ASSERT(rc == RC::SUCCESS, "hash failed");
    return ret;
  }
};

}  // namespace std

class HashJoinHashTable
{
public:
  HashJoinHashTable() = default;
  void insert(const HashJoinKey &key, const ValueListTuple &value) { ht_.insert({key, value}); }

  void Remove(const HashJoinKey &key) { ht_.erase(key); }

  struct Iterator
  {
  public:
    Iterator() = default;
    explicit Iterator(std::unordered_multimap<HashJoinKey, ValueListTuple>::iterator iter) : iter_{iter} {}

    auto key() -> const HashJoinKey & { return iter_->first; }

    auto val() -> ValueListTuple & { return iter_->second; }

    auto operator++() -> Iterator &
    {
      ++iter_;
      return *this;
    }

    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

  private:
    std::unordered_multimap<HashJoinKey, ValueListTuple>::iterator iter_;
  };

  auto begin() -> Iterator { return Iterator{ht_.begin()}; }
  auto end() -> Iterator { return Iterator{ht_.end()}; }

  auto find(const HashJoinKey &key) -> std::pair<Iterator, Iterator>
  {
    auto [begin_iter, end_iter] = ht_.equal_range(key);
    return {Iterator{begin_iter}, Iterator{end_iter}};
  }

private:
  unordered_multimap<HashJoinKey, ValueListTuple> ht_;
};

/**
 * @brief Hash Join 算子
 * @ingroup PhysicalOperator
 */
class HashJoinPhysicalOperator : public PhysicalOperator
{
public:
  HashJoinPhysicalOperator()          = default;
  virtual ~HashJoinPhysicalOperator() = default;

  string param() const override { 
    string ret = left_key_exprs_.at(0)->to_string();
    ret.push_back('=');
    ret.append(right_key_exprs_.at(0)->to_string());
    return ret;
  }

  PhysicalOperatorType type() const override { return PhysicalOperatorType::HASH_JOIN; }

  OpType get_op_type() const override { return OpType::INNERHASHJOIN; }

  virtual double calculate_cost(
      LogicalProperty *prop, const vector<LogicalProperty *> &child_log_props, CostModel *cm) override
  {
    return 0.0;
  }

  RC     open(Trx *trx) override;
  RC     next() override;
  RC     close() override;
  Tuple *current_tuple() override;

  vector<unique_ptr<Expression>>       &left_key_exprs() { return left_key_exprs_; }
  const vector<unique_ptr<Expression>> &left_key_exprs() const { return left_key_exprs_; }
  vector<unique_ptr<Expression>>       &right_key_exprs() { return right_key_exprs_; }
  const vector<unique_ptr<Expression>> &right_key_exprs() const { return right_key_exprs_; }

private:
  auto build_hash_table() -> RC;

  Trx *trx_ = nullptr;

  PhysicalOperator              *left_        = nullptr;
  PhysicalOperator              *right_       = nullptr;
  Tuple                         *left_tuple_  = nullptr;
  Tuple                         *right_tuple_ = nullptr;
  JoinedTuple                    joined_tuple_;  //! 当前关联的左右两个tuple
  vector<unique_ptr<Expression>> left_key_exprs_;
  vector<unique_ptr<Expression>> right_key_exprs_;

  HashJoinHashTable           ht_;
  HashJoinHashTable::Iterator begin_iter_;
  HashJoinHashTable::Iterator end_iter_;
};
