#include "sql/parser/binder_context.h"
#include "sql/parser/parse_defs.h"

void BinderContext::add_current_data_source(Table *table)
{
  data_source_map_.insert({table->name(), table});
  if (!unique_data_sources_.contains(table)) {
    unique_data_sources_.insert(table);
  }

  for (auto cur_table : current_data_sources_) {
    if (cur_table == DataSource{table}) {
      return;
    }
  }
  current_data_sources_.emplace_back(table);
}

void BinderContext::add_current_data_source(const string &alias_name, Table *table)
{
  data_source_map_.insert({alias_name, table});
  if (!unique_data_sources_.contains(table)) {
    unique_data_sources_.insert(table);
  }

  for (auto cur_table : current_data_sources_) {
    if (cur_table == DataSource{table}) {
      return;
    }
  }
  current_data_sources_.emplace_back(table);
}

void BinderContext::add_outer_data_source(Table *table) { outer_data_sources_.emplace_back(table); }

void BinderContext::add_used_outer_data_source(Table *table) { used_outer_data_sources_.emplace_back(table); }


void BinderContext::add_current_data_source(View *view) {
  data_source_map_.insert({view->name(), view});
  if (!unique_data_sources_.contains(view)) {
    unique_data_sources_.insert(view);
  }

  for (auto cur_view : current_data_sources_) {
    if (cur_view == DataSource{view}) {
      return;
    }
  }
  current_data_sources_.emplace_back(view);
}

void BinderContext::add_current_data_source(const string &alias_name, View *view) {
  data_source_map_.insert({alias_name, view});
  if (!unique_data_sources_.contains(view)) {
    unique_data_sources_.insert(view);
  }

  for (auto cur_view : current_data_sources_) {
    if (cur_view == DataSource{view}) {
      return;
    }
  }
  current_data_sources_.emplace_back(view);
}

void BinderContext::add_outer_data_source(View *view) {
  outer_data_sources_.emplace_back(view);
}

void BinderContext::add_used_outer_data_source(View *view) {
  used_outer_data_sources_.emplace_back(view);
}

void BinderContext::add_outer_data_source(const DataSource& ds) {
  outer_data_sources_.push_back(ds);
}

void BinderContext::add_used_outer_data_source(const DataSource& ds) {
  used_outer_data_sources_.push_back(ds);
}

void BinderContext::clear_current_data_sources()
{
  current_data_sources_.clear();
  data_source_map_.clear();
}

void BinderContext::clear_outer_data_sources() { outer_data_sources_.clear(); }

DataSource BinderContext::find_current_data_source(const char *table_name) const
{
  if (!data_source_map_.contains(table_name)) {
    return DataSource{};
  }
  DataSource target = data_source_map_.at(table_name);
  for (auto& ds : current_data_sources_) {
    if (ds == target) {
      return ds;
    }
  }
  return DataSource{};
}

DataSource BinderContext::find_outer_data_source(const char *table_name) const
{
  if (!data_source_map_.contains(table_name)) {
    return DataSource{};
  }
  // Table* target = data_source_map_.at(table_name);
  DataSource target = data_source_map_.at(table_name);
  for (auto& ds : outer_data_sources_) {
    if (ds == target) {
      return ds;
    }
  }
  return DataSource{};
}
