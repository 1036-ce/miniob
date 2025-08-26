#include "sql/parser/binder_context.h"
#include "sql/parser/parse_defs.h"

void BinderContext::add_current_data_source(Table *table)
{
  data_source_map_.insert({table->name(), table});
  current_ds_names_.push_back(table->name());
}

void BinderContext::add_current_data_source(const string &alias_name, Table *table)
{
  data_source_map_.insert({alias_name, table});
  current_ds_names_.push_back(alias_name);
}

void BinderContext::add_current_data_source(View *view)
{
  data_source_map_.insert({view->name(), view});
  current_ds_names_.push_back(view->name());
}

void BinderContext::add_current_data_source(const string &alias_name, View *view)
{
  data_source_map_.insert({alias_name, view});
  current_ds_names_.push_back(alias_name);
}

void BinderContext::add_current_data_source(const DataSource &ds)
{
  data_source_map_.insert({ds.name(), ds});
  current_ds_names_.push_back(ds.name());
}

void BinderContext::add_current_data_source(const string &alias_name, const DataSource &ds)
{
  data_source_map_.insert({alias_name, ds});
  current_ds_names_.push_back(alias_name);
}

void BinderContext::add_outer_data_source(const string &name) { outer_ds_names_.push_back(name); }

void BinderContext::add_used_outer_data_source(const string &name) { used_outer_ds_names_.push_back(name); }

DataSource BinderContext::find_current_data_source(const char *name) const
{
  for (auto &ds_name : current_ds_names_) {
    if (ds_name == name) {
      return data_source_map_.at(ds_name);
    }
  }
  return DataSource{};
}

DataSource BinderContext::find_outer_data_source(const char *name) const
{
  for (auto &ds_name : outer_ds_names_) {
    if (ds_name == name) {
      return data_source_map_.at(ds_name);
    }
  }
  return DataSource{};
}

bool BinderContext::contains(const char *name) const {
  return data_source_map_.contains(name);
}

vector<DataSource> BinderContext::current_data_sources() const
{
  vector<DataSource> ret;
  for (auto &name : current_ds_names_) {
    ret.push_back(data_source_map_.at(name));
  }
  return ret;
}

vector<DataSource> BinderContext::outer_data_sources() const
{
  vector<DataSource> ret;
  for (auto &name : outer_ds_names_) {
    ret.push_back(data_source_map_.at(name));
  }
  return ret;
}

vector<DataSource> BinderContext::used_outer_data_sources() const
{
  vector<DataSource> ret;
  for (auto &name : used_outer_ds_names_) {
    ret.push_back(data_source_map_.at(name));
  }
  return ret;
}
