#include "sql/executor/create_view_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/create_view_stmt.h"
#include "storage/db/db.h"

RC CreateViewExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  Db      *db      = session->get_current_db();
  ASSERT(stmt->type() == StmtType::CREATE_VIEW,
      "create view executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  RC rc = RC::SUCCESS;

  CreateViewStmt *create_view_stmt = static_cast<CreateViewStmt *>(stmt);
  LOG_DEBUG("view's name: %s, view's select_sql: %s", create_view_stmt->view_name().c_str(), create_view_stmt->select_sql().c_str());
  if (create_view_stmt->attr_names().empty()) {
    if (OB_FAIL(rc = db->create_view(create_view_stmt->view_name(), create_view_stmt->select_sql()))) {
      return rc;
    }
  } else {
    if (OB_FAIL(rc = db->create_view(
                    create_view_stmt->view_name(), create_view_stmt->attr_names(), create_view_stmt->select_sql()))) {
      return rc;
    }
  }
  return rc;
}
