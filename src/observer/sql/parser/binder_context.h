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
  void add_outer_data_source(Table *table);
  void add_used_outer_data_source(Table *table);

  void add_current_data_source(View *view);
  void add_current_data_source(const string &alias_name, View *view);
  void add_outer_data_source(View *view);
  void add_used_outer_data_source(View *view);

  void add_outer_data_source(const DataSource& ds);
  void add_used_outer_data_source(const DataSource& ds);

  void clear_current_data_sources();
  void clear_outer_data_sources();

  DataSource find_current_data_source(const char *table_name) const;
  DataSource find_outer_data_source(const char *table_name) const;

  const vector<DataSource> &current_data_sources() const { return current_data_sources_; }
  const vector<DataSource> &outer_data_sources() const { return outer_data_sources_; }
  const vector<DataSource> &used_outer_data_sources() const { return used_outer_data_sources_; }

  void set_db(Db *db) { db_ = db; }
  Db  *db() { return db_; }

  BinderContext gen_sub_context() const
  {
    BinderContext sub_context;
    sub_context.db_                 = this->db_;
    sub_context.data_source_map_          = this->data_source_map_;
    sub_context.unique_data_sources_      = this->unique_data_sources_;
    sub_context.outer_data_sources_ = this->outer_data_sources_;
    for (auto table : this->current_data_sources_) {
      sub_context.outer_data_sources_.push_back(table);
    }
    sub_context.used_outer_data_sources_.clear();
    return sub_context;
  }

private:
  Db             *db_ = nullptr;
  vector<DataSource> current_data_sources_;
  vector<DataSource> outer_data_sources_;
  vector<DataSource> used_outer_data_sources_;

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

  unordered_map<string, DataSource, Hash, EqualTo> data_source_map_;
  unordered_set<DataSource>                        unique_data_sources_;
};

