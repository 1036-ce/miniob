#pragma once

#include "storage/table/table.h"
#include "storage/view/view.h"

class DataSource
{
public:
  DataSource() = default;
  DataSource(Table *table) : table_(table) {}
  DataSource(View *view) : view_(view) {}

  bool operator==(const DataSource& rhs) const {
    return this->table_ == rhs.table_ && this->view_ == rhs.view_;
  }

  bool operator!=(const DataSource& rhs) const {
    return !(*this == rhs);
  }

  operator bool() const {
    return is_valid();
  }

  bool is_valid() const {
    return (table_ == nullptr && view_ != nullptr) || (table_ != nullptr && view_ == nullptr);
  }

  Table *table() const { return table_; }
  View  *view() const { return view_; }

  string name() const
  {
    if (table_) {
      return table_->name();
    }
    return view_->name();
  }

private:
  Table *table_ = nullptr;
  View  *view_  = nullptr;
};

template <>
struct std::hash<DataSource>
{
  std::size_t operator()(const DataSource &ds) const {
    std::size_t h1 = std::hash<Table*>()(ds.table());
    std::size_t h2 = std::hash<View*>()(ds.view());
    return h1 ^ (h2 << 1);
  }
};
