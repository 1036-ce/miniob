#pragma once

#include "sql/expr/expression.h"
#include "storage/view/view.h"

class ViewFieldExpr : public Expression
{
public:
  ViewFieldExpr() = default;
  ViewFieldExpr(const string &view_name, const ViewFieldMeta &view_field_meta)
      : view_name_(view_name), view_field_meta_(view_field_meta)
  {}

  virtual ~ViewFieldExpr() = default;

  unique_ptr<Expression> copy() const override { return make_unique<ViewFieldExpr>(view_name_, view_field_meta_); }

  ExprType type() const override { return ExprType::VIEW_FIELD; }
  AttrType value_type() const override { return view_field_meta_.type(); }
  int      value_length() const override { return view_field_meta_.length(); }

  const string &view_name() const { return view_name_; }
  const string &field_name() const { return view_field_meta_.name(); }

  RC get_column(Chunk &chunk, Column &column) override;

  RC get_value(const Tuple &tuple, Value &value) const override;

  RC related_tables(vector<const Table *> &tables) const override;

  string to_string() const override { return view_name_ + "." + view_field_meta_.name(); }

private:
  string        view_name_;
  ViewFieldMeta view_field_meta_;
};
