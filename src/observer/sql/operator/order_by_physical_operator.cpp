#include "sql/operator/order_by_physical_operator.h"
#include "sql/operator/physical_operator.h"

RC OrderByPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "order by operator only support one child, but got %d", children_.size());
  PhysicalOperator &child = *children_.front();
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  size_t counter = 0;
  while (OB_SUCC(rc = child.next())) {
    Tuple *child_tuple = child.current_tuple();
    if (nullptr == child_tuple) {
      LOG_WARN("failed to get tuple from child operator. rc=%s", strrc(rc));
      return RC::INTERNAL;
    }

    ValueListTuple value_list_tuple;
    ValueListTuple::make(*child_tuple, value_list_tuple);
    tuples_.push_back(value_list_tuple);

    vector<Value> sort_key;
    for (auto& orderby: orderbys_) {
      Value val;
      if (OB_FAIL(orderby->expr->get_value(value_list_tuple, val))) {
        return rc;
      }
      sort_key.push_back(val);
    }
    sort_keys_.push_back(sort_key);
    ids_.push_back(counter++);
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

  auto comp = [&](const size_t& lhs, const size_t& rhs) -> bool {
    const auto& left_key = sort_keys_.at(lhs);
    const auto& right_key = sort_keys_.at(rhs);

    for (size_t i = 0; i < orderbys_.size(); ++i) {
      const auto& orderby = orderbys_.at(i);
      const auto& left_val = left_key.at(i);
      const auto& right_val = right_key.at(i);

      if (left_val.is_null()) {
        if (right_val.is_null()) {
          continue;
        }
        return orderby->is_asc;
      }
      else if (right_val.is_null()) {
        return !orderby->is_asc;
      }

      int res = left_val.compare(right_val);
      if (res == 0) {
        continue;
      }

      return orderby->is_asc ? res < 0 : res > 0;
    }
    return false;
  };
  std::sort(ids_.begin(), ids_.end(), comp);
  iter_ = 0;
  first_emit_ = true;

  return rc;
}

RC OrderByPhysicalOperator::next()
{
  if (first_emit_) {
    first_emit_ = false;
    if (iter_ == ids_.size()) {
      return RC::RECORD_EOF;
    }
    return RC::SUCCESS;
  }

  ++iter_;
  if (iter_ == ids_.size()) {
    return RC::RECORD_EOF;
  }
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close()
{
  return RC::SUCCESS;
}

Tuple *OrderByPhysicalOperator::current_tuple() {
  size_t id = ids_[iter_];
  return &tuples_.at(id);
}
