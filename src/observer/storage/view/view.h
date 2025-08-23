#pragma once

#include "common/lang/string.h"
#include "common/lang/serializable.h"
#include "common/sys/rc.h"
#include "sql/operator/logical_operator.h"

class SelectStmt;

class ViewFieldMeta
{
public:
  ViewFieldMeta() = default;
  ViewFieldMeta(const Expression &expr)
  {
    if (expr.type() == ExprType::TABLE_FIELD) {
      auto& table_field_expr = static_cast<const TableFieldExpr&>(expr);
      name_                 = table_field_expr.name();
      original_table_name_  = table_field_expr.table_name();
      original_field_name_  = table_field_expr.field_name(); 
    }
    else {
      name_ = expr.name();
      original_table_name_ = "";
      original_field_name_ = "";
    }
  }
  ViewFieldMeta(const string &name, const string &original_table_name, const string &original_field_name)
      : name_(name), original_table_name_(original_table_name), original_field_name_(original_table_name)
  {}
  ~ViewFieldMeta() = default;

  RC init(const string &name, const string &original_table_name, const string &original_field_name)
  {
    name_                = name;
    original_table_name_ = original_table_name;
    original_field_name_ = original_field_name;
    return RC::SUCCESS;
  }

  const string &name() const { return name_; }
  const string &original_table_name() const { return original_table_name_; }
  const string &original_field_name() const { return original_field_name_; }

  void      to_json(Json::Value &json_value) const;
  static RC from_json(const Json::Value &json_value, ViewFieldMeta &field);

private:
  string name_;
  string original_table_name_;
  string original_field_name_;
};

/**
 * @brief 视图
 */
class View : public common::Serializable
{
public:
  View()          = default;
  virtual ~View() = default;

  RC create(Db *db, const char *path, const char *name, const char *base_dir, const string &select_sql);
  RC open(Db *db, const char *name, const char *base_dir);

  /* RC gen_select_stmt();
   * RC gen_logical_plan();
   * RC gen_physical_plan(); */

  const string                &name() const { return name_; }
  const string                &select_sql() const{ return select_sql_; }
  const vector<ViewFieldMeta> &field_metas() const { return field_metas_; }

private:
  RC create_select_stmt(Db *db, const string &select_sql, unique_ptr<SelectStmt> &select_stmt);

  int  serialize(ostream &os) const override;
  int  deserialize(istream &is) override;
  int  get_serial_size() const override;
  void to_string(string &output) const override;

  Db                   *db_ = nullptr;
  string                name_;
  string                select_sql_;
  vector<ViewFieldMeta> field_metas_;
};
