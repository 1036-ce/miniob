#pragma once

#include "common/lang/string.h"
#include "common/sys/rc.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/select_stmt.h"

/**
 * @brief 视图
 */
class View
{
public:
  View() = default;
  ~View() = default;

  RC create(Db* db, const char *path, const char *name, const char *base_dir, const string& select_sql);
  RC open(Db* db, const char *name, const char *base_dir);

  RC get_select_stmt();
  RC get_logical_plan();
  RC get_physical_plan();

private:
  RC create_select_stmt(Db* db, const string& select_sql);

  string select_sql_;
};
