#pragma once

#include "sql/operator/logical_operator.h"
#include "common/types.h"
#include "storage/view/view.h"

class ViewGetLogicalOperator : public LogicalOperator
{
public:
  ViewGetLogicalOperator(View* view): view_(view) {}
  virtual ~ViewGetLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::VIEW_GET; }

  OpType get_op_type() const override { return OpType::LOGICALGET; }

  View* view() const { return view_; }

private:
  View *view_ = nullptr;
};
