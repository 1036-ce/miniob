#include "sql/operator/order_by_physical_operator.h"
#include "sql/operator/physical_operator.h"

void TopNHeap::insert(const SortEntry &sort_entry)
{
  if (heap_.size() < max_size_) {
    heap_.push_back(sort_entry);
    float_up();
  } else {
    // sort_entry > top()
    const auto &top_entry = top();
    if (comp_(top_entry, sort_entry)) {
      return;
    }
    heap_[0] = sort_entry;
    sink_down();
  }
}

void TopNHeap::pop()
{
  std::swap(heap_.front(), heap_.back());
  heap_.pop_back();
  sink_down();
}

const SortEntry &TopNHeap::top() const { return heap_.at(0); }

void TopNHeap::float_up()
{
  if (heap_.size() == 0) {
    return;
  }

  size_t index        = heap_.size() - 1;
  size_t parent_index = parent(index);

  while (index > 0) {
    auto &cur_entry    = heap_.at(index);
    auto &parent_entry = heap_.at(parent_index);

    // parent < cur
    if (comp_(parent_entry, cur_entry)) {
      std::swap(cur_entry, parent_entry);
      index        = parent_index;
      parent_index = parent(index);
    } else {
      break;
    }
  }
}

void TopNHeap::sink_down()
{
  if (heap_.size() == 0) {
    return;
  }

  size_t index       = 0;
  size_t left_index  = left_child(index);
  size_t right_index = right_child(index);

  while (left_index < heap_.size()) {
    auto &cur_entry  = heap_.at(index);
    auto &left_entry = heap_.at(left_index);

    if (right_index < heap_.size()) {
      auto &right_entry = heap_.at(right_index);
      // left > cur || right > cur
      if (comp_(cur_entry, left_entry) || comp_(cur_entry, right_entry)) {
        auto target_index = left_index;
        // right > left
        if (comp_(left_entry, right_entry)) {
          target_index = right_index;
        }
        std::swap(heap_.at(index), heap_.at(target_index));
        index = target_index;
      } else {
        break;
      }
    } else {
      // left > cur
      if (comp_(cur_entry, left_entry)) {
        std::swap(cur_entry, left_entry);
        index = left_index;
      } else {
        break;
      }
    }

    left_index  = left_child(index);
    right_index = right_child(index);
  }
}

RC OrderByPhysicalOperator::open(Trx *trx)
{
  if (limit_ == -1) {
    return non_limit_open(trx);
  }
  if (limit_ == 0) {
    first_emit_ = true;
    iter_       = 0;
    return RC::SUCCESS;
  }

  return limit_open(trx);
}

RC OrderByPhysicalOperator::next()
{
  /*   if (first_emit_) {
   *     first_emit_ = false;
   *     if (iter_ == ids_.size()) {
   *       return RC::RECORD_EOF;
   *     }
   *     return RC::SUCCESS;
   *   }
   *
   *   ++iter_;
   *   if (iter_ == ids_.size()) {
   *     return RC::RECORD_EOF;
   *   }
   *   return RC::SUCCESS; */
  if (first_emit_) {
    first_emit_ = false;
    if (iter_ == sort_entries_.size()) {
      return RC::RECORD_EOF;
    }
    return RC::SUCCESS;
  }

  ++iter_;
  if (iter_ == sort_entries_.size()) {
    return RC::RECORD_EOF;
  }
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close() { return RC::SUCCESS; }

Tuple *OrderByPhysicalOperator::current_tuple()
{
  /* size_t id = ids_[iter_];
   * return &tuples_.at(id); */
  return &sort_entries_.at(iter_).tuple();
}

RC OrderByPhysicalOperator::limit_open(Trx *trx)
{

  ASSERT(children_.size() == 1, "order by operator only support one child, but got %d", children_.size());
  PhysicalOperator &child = *children_.front();
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  TopNHeap heap{static_cast<size_t>(limit_), orderbys_};

  while (OB_SUCC(rc = child.next())) {
    Tuple *child_tuple = child.current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }

    ValueListTuple value_list_tuple;
    ValueListTuple::make(*child_tuple, value_list_tuple);

    vector<Value> sort_key;
    for (auto &orderby : orderbys_) {
      Value val;
      if (OB_FAIL(orderby->expr->get_value(value_list_tuple, val))) {
        return rc;
      }
      sort_key.push_back(val);
    }

    SortEntry sort_entry{std::move(sort_key), std::move(value_list_tuple)};
    heap.insert(sort_entry);
  }

  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  }

  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get next tuple. rc=%s", strrc(rc));
    return rc;
  }

  if (OB_FAIL(rc = child.close())) {
    LOG_WARN("failed to close child. rc=%s", strrc(rc));
    return rc;
  }

  // std::sort(sort_entries_.begin(), sort_entries_.end(), comp_);
  // sort_entries_ = std::move(heap.heap());
  while (!heap.empty()) {
    sort_entries_.push_back(heap.top());
    heap.pop();
  }
  std::reverse(sort_entries_.begin(), sort_entries_.end());
  iter_       = 0;
  first_emit_ = true;

  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::non_limit_open(Trx *trx)
{
  ASSERT(children_.size() == 1, "order by operator only support one child, but got %d", children_.size());
  PhysicalOperator &child = *children_.front();
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child.next())) {
    Tuple *child_tuple = child.current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }

    ValueListTuple value_list_tuple;
    ValueListTuple::make(*child_tuple, value_list_tuple);

    vector<Value> sort_key;
    for (auto &orderby : orderbys_) {
      Value val;
      if (OB_FAIL(orderby->expr->get_value(value_list_tuple, val))) {
        return rc;
      }
      sort_key.push_back(val);
    }
    sort_entries_.emplace_back(std::move(sort_key), std::move(value_list_tuple));
  }

  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  }

  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get next tuple. rc=%s", strrc(rc));
    return rc;
  }

  if (OB_FAIL(rc = child.close())) {
    LOG_WARN("failed to close child. rc=%s", strrc(rc));
    return rc;
  }

  std::sort(sort_entries_.begin(), sort_entries_.end(), comp_);
  iter_       = 0;
  first_emit_ = true;

  return RC::SUCCESS;
}

/* RC OrderByPhysicalOperator::limit_open(Trx *trx) {
 *   ASSERT(children_.size() == 1, "order by operator only support one child, but got %d", children_.size());
 *   PhysicalOperator &child = *children_.front();
 *   RC                rc    = child.open(trx);
 *   if (OB_FAIL(rc)) {
 *     LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
 *     return rc;
 *   }
 *
 *   size_t counter = 0;
 *   while (OB_SUCC(rc = child.next())) {
 *     Tuple *child_tuple = child.current_tuple();
 *     if (nullptr == child_tuple) {
 *       LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
 *       return RC::INTERNAL;
 *     }
 *
 *     ValueListTuple value_list_tuple;
 *     ValueListTuple::make(*child_tuple, value_list_tuple);
 *     tuples_.push_back(value_list_tuple);
 *
 *     vector<Value> sort_key;
 *     for (auto& orderby: orderbys_) {
 *       Value val;
 *       if (OB_FAIL(orderby->expr->get_value(value_list_tuple, val))) {
 *         return rc;
 *       }
 *       sort_key.push_back(val);
 *     }
 *     sort_keys_.push_back(sort_key);
 *     ids_.push_back(counter++);
 *   }
 *
 *   if (RC::RECORD_EOF == rc) {
 *     rc = RC::SUCCESS;
 *   }
 *
 *   if (OB_FAIL(rc)) {
 *     LOG_WARN("failed to get next tuple. rc=%s", strrc(rc));
 *     return rc;
 *   }
 *
 *   if (OB_FAIL(rc = child.close())) {
 *     LOG_WARN("failed to close child. rc=%s", strrc(rc));
 *     return rc;
 *   }
 *
 *   return RC::SUCCESS;
 * } */

/* RC OrderByPhysicalOperator::non_limit_open(Trx *trx) {
 *   ASSERT(children_.size() == 1, "order by operator only support one child, but got %d", children_.size());
 *   PhysicalOperator &child = *children_.front();
 *   RC                rc    = child.open(trx);
 *   if (OB_FAIL(rc)) {
 *     LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
 *     return rc;
 *   }
 *
 *   size_t counter = 0;
 *   while (OB_SUCC(rc = child.next())) {
 *     Tuple *child_tuple = child.current_tuple();
 *     if (nullptr == child_tuple) {
 *       LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
 *       return RC::INTERNAL;
 *     }
 *
 *     ValueListTuple value_list_tuple;
 *     ValueListTuple::make(*child_tuple, value_list_tuple);
 *     tuples_.push_back(value_list_tuple);
 *
 *     vector<Value> sort_key;
 *     for (auto& orderby: orderbys_) {
 *       Value val;
 *       if (OB_FAIL(orderby->expr->get_value(value_list_tuple, val))) {
 *         return rc;
 *       }
 *       sort_key.push_back(val);
 *     }
 *     sort_keys_.push_back(sort_key);
 *     ids_.push_back(counter++);
 *   }
 *
 *   if (RC::RECORD_EOF == rc) {
 *     rc = RC::SUCCESS;
 *   }
 *
 *   if (OB_FAIL(rc)) {
 *     LOG_WARN("failed to get next tuple. rc=%s", strrc(rc));
 *     return rc;
 *   }
 *
 *   if (OB_FAIL(rc = child.close())) {
 *     LOG_WARN("failed to close child. rc=%s", strrc(rc));
 *     return rc;
 *   }
 *
 *   auto comp = [&](const size_t& lhs, const size_t& rhs) -> bool {
 *     const auto& left_key = sort_keys_.at(lhs);
 *     const auto& right_key = sort_keys_.at(rhs);
 *
 *     for (size_t i = 0; i < orderbys_.size(); ++i) {
 *       const auto& orderby = orderbys_.at(i);
 *       const auto& left_val = left_key.at(i);
 *       const auto& right_val = right_key.at(i);
 *
 *       if (left_val.is_null()) {
 *         if (right_val.is_null()) {
 *           continue;
 *         }
 *         return orderby->is_asc;
 *       }
 *       else if (right_val.is_null()) {
 *         return !orderby->is_asc;
 *       }
 *
 *       int res = left_val.compare(right_val);
 *       if (res == 0) {
 *         continue;
 *       }
 *
 *       return orderby->is_asc ? res < 0 : res > 0;
 *     }
 *     return false;
 *   };
 *   std::sort(ids_.begin(), ids_.end(), comp);
 *   iter_ = 0;
 *   first_emit_ = true;
 *
 *   return rc;
 * } */
