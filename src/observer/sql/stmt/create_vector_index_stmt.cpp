
#include "sql/stmt/create_vector_index_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

using namespace std;
using namespace common;

RC CreateVectorIndexStmt::create(Db *db, const CreateVectorIndexSqlNode &create_vector_index, Stmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt                     = nullptr;
  const string &index_name = create_vector_index.index_name;
  const char   *table_name = create_vector_index.relation_name.c_str();
  const string &attr_name  = create_vector_index.attr_name;

  if (is_blank(table_name) || is_blank(index_name.c_str())) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, index name=%s",
        db, table_name, index_name.c_str());
    return RC::INVALID_ARGUMENT;
  }
  if (is_blank(attr_name.c_str())) {
    LOG_WARN("invalid argument. attribute name=%s", attr_name.c_str());
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  Index *index = table->find_index(index_name.c_str());
  if (nullptr != index) {
    LOG_WARN("index with name(%s) already exists. table name=%s", index_name.c_str(), table_name);
    return RC::SCHEMA_INDEX_NAME_REPEAT;
  }

  const FieldMeta *field_meta = table->table_meta().field(attr_name.c_str());
  if (nullptr == field_meta) {
    LOG_WARN("no such field in table. db=%s, table=%s, field=%s", db->name(), table_name, attr_name.c_str());
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  unordered_map<string, string> params;
  if (OB_FAIL(rc = gen_params(create_vector_index.param_list, params))) {
    return rc;
  }

  stmt = new CreateVectorIndexStmt(table, *field_meta, index_name, params);

  return RC::SUCCESS;
}

RC CreateVectorIndexStmt::gen_params(const vector<unique_ptr<Param>> &param_list, unordered_map<string, string> &params)
{
  for (const auto &param : param_list) {
    string new_key = param->key;
    str_to_lower(new_key);
    if (params.contains(new_key)) {
      LOG_WARN("multi param's key: %s", param->key.c_str());
      return RC::INVALID_ARGUMENT;
    }
    string new_val = param->value;
    str_to_lower(new_val);
    params.insert({new_key, new_val});
  }
  return RC::SUCCESS;
}
