#include "common/log/log.h"
#include "common/types.h"
#include "sql/stmt/create_view_stmt.h"
#include "event/sql_debug.h"

RC CreateViewStmt::create(Db *db, const CreateViewSqlNode &create_view, Stmt *&stmt) {
  stmt = new CreateViewStmt(create_view.view_name, create_view.attr_names, create_view.select_sql);
  return RC::SUCCESS;
}
