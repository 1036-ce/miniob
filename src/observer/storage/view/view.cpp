#include "storage/view/view.h"
#include "sql/parser/parse.h"
#include "storage/common/meta_util.h"

RC View::create(Db* db, const char *path, const char *name, const char *base_dir, const string& select_sql) {
  RC rc = RC::SUCCESS;
  // 判断视图文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create view file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create view file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN;
  }

  close(fd);
  fstream fs;
  fs.open(path, ios_base::out | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }
  fs << select_sql;

  /* if (OB_FAIL(rc = create_select_stmt(db, select_sql))) {
   *   return rc;
   * } */

  select_sql_ = select_sql;
  LOG_INFO("Successfully create view %s:%s", base_dir, name);
  return rc;
}

RC View::open(Db* db, const char *name, const char *base_dir) {
  RC rc = RC::SUCCESS;
  string path = string(base_dir) + common::FILE_PATH_SPLIT_STR + name;
  string select_sql;
  fstream fs;
  fs.open(path, ios_base::out | ios_base::binary);
  fs >> select_sql_;

  return rc;
}

RC View::create_select_stmt(Db* db, const string& select_sql) {
  RC rc = RC::SUCCESS;
  ParsedSqlResult parsed_sql_result;

  parse(select_sql.c_str(), &parsed_sql_result);
  if (parsed_sql_result.sql_nodes().empty()) {
    return RC::INTERNAL;
  }
  if (parsed_sql_result.sql_nodes().size() > 1) {
    LOG_WARN("got multi sql commands but only 1 will be handled");
  }

  unique_ptr<ParsedSqlNode> sql_node = std::move(parsed_sql_result.sql_nodes().front());
  if (sql_node->flag == SCF_ERROR) {
    return RC::SQL_SYNTAX;
  }

  Stmt          *stmt     = nullptr;
  if (OB_FAIL(rc = Stmt::create_stmt(db, *sql_node, stmt))) {
    return rc;
  }
  // select_stmt_.reset(static_cast<SelectStmt*>(stmt));

  return rc;
}
