#pragma once

#include "storage/data_source/data_source.h"
#include "storage/table/table.h"

class BinderContext
{
public:
  BinderContext()          = default;
  virtual ~BinderContext() = default;

  void add_current_data_source(Table *table);
  void add_current_data_source(const string &alias_name, Table *table);

  void add_current_data_source(View *view);
  void add_current_data_source(const string &alias_name, View *view);

  void add_current_data_source(const DataSource &ds);
  void add_current_data_source(const string &alias_name, const DataSource &ds);
  void add_outer_data_source(const string &name);
  void add_used_outer_data_source(const string &name);

  DataSource find_current_data_source(const char *name) const;
  DataSource find_outer_data_source(const char *name) const;

  vector<DataSource> current_data_sources() const;
  vector<DataSource> outer_data_sources() const;
  vector<DataSource> used_outer_data_sources() const;

  const vector<string> &current_ds_names() const { return current_ds_names_; }
  const vector<string> &outer_ds_names() const { return outer_ds_names_; }
  const vector<string> &used_outer_ds_names() const { return used_outer_ds_names_; }

  void set_db(Db *db) { db_ = db; }
  Db  *db() { return db_; }

  BinderContext gen_sub_context() const
  {
    BinderContext sub_context;
    sub_context.db_              = this->db_;
    // sub_context.ds_map_ = this->ds_map_;
    sub_context.outer_ds_map_ = this->outer_ds_map_;
    for (const auto& [k, v]: this->current_ds_map_) {
      sub_context.outer_ds_map_.insert({k, v});
    }

    sub_context.outer_ds_names_ = this->outer_ds_names_;
    for (auto table : this->current_ds_names_) {
      sub_context.outer_ds_names_.push_back(table);
    }
    sub_context.used_outer_ds_names_.clear();
    return sub_context;
  }

private:
  Db            *db_ = nullptr;
  vector<string> current_ds_names_;
  vector<string> outer_ds_names_;
  vector<string> used_outer_ds_names_;

  struct Hash
  {
    std::size_t operator()(const string &s) const
    {
      string t = s;
      for (char &c : t) {
        c = std::tolower(c);
      }
      return std::hash<string>{}(t);
    }
  };

  struct EqualTo
  {
    bool operator()(const string &lhs, const string &rhs) const { return 0 == strcasecmp(lhs.c_str(), rhs.c_str()); }
  };

  unordered_map<string, DataSource, Hash, EqualTo> current_ds_map_;
  unordered_map<string, DataSource, Hash, EqualTo> outer_ds_map_;
};
