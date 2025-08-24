#pragma once

#include "common/types.h"
#include "sql/stmt/stmt.h"

class CreateViewStmt : public Stmt
{
public:
  // CreateViewStmt(const string &view_name, const string &select_sql) : view_name_(view_name), select_sql_(select_sql)
  // {}
  CreateViewStmt(const string &view_name, const vector<string> &attr_names, const string &select_sql)
      : view_name_(view_name), attr_names_(attr_names), select_sql_(select_sql)
  {}

  virtual ~CreateViewStmt() = default;

  StmtType type() const override { return StmtType::CREATE_VIEW; }

  static RC create(Db *db, const CreateViewSqlNode &create_view, Stmt *&stmt);

  const string         &view_name() { return view_name_; }
  const vector<string> &attr_names() { return attr_names_; }
  const string         &select_sql() { return select_sql_; }

private:
  string         view_name_;
  vector<string> attr_names_;
  string         select_sql_;
};
