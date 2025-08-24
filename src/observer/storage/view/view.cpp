#include "storage/view/view.h"
#include "sql/operator/project_physical_operator.h"
#include "sql/operator/view_project_physical_operator.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/parser/parse.h"
#include "sql/stmt/select_stmt.h"
#include "storage/common/meta_util.h"

#include "json/json.h"

const static Json::StaticString VIEW_FIELD_NAME("name");
const static Json::StaticString VIEW_FIELD_ORIGINAL_TABLE_NAME("original_table_name");
const static Json::StaticString VIEW_FIELD_ORIGINAL_FIELD_NAME("original_field_name");
const static Json::StaticString VIEW_FIELD_TYPE("type");
const static Json::StaticString VIEW_FIELD_LENGTH("length");

const static Json::StaticString VIEW_NAME("name");
const static Json::StaticString VIEW_SELECT_SQL("select_sql");
const static Json::StaticString VIEW_FIELDS("fields");

void ViewFieldMeta::to_json(Json::Value &json_value) const
{
  json_value[VIEW_FIELD_NAME]                = name_;
  json_value[VIEW_FIELD_ORIGINAL_TABLE_NAME] = original_table_name_;
  json_value[VIEW_FIELD_ORIGINAL_FIELD_NAME] = original_field_name_;
  json_value[VIEW_FIELD_TYPE]                = attr_type_to_string(type_);
  json_value[VIEW_FIELD_LENGTH]              = length_;
}

RC ViewFieldMeta::from_json(const Json::Value &json_value, ViewFieldMeta &field)
{
  if (!json_value.isObject()) {
    LOG_ERROR("Failed to deserialize field. json is not an object. json value=%s", json_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  const Json::Value &name_value                = json_value[VIEW_FIELD_NAME];
  const Json::Value &original_table_name_value = json_value[VIEW_FIELD_ORIGINAL_TABLE_NAME];
  const Json::Value &original_field_name_value = json_value[VIEW_FIELD_ORIGINAL_FIELD_NAME];
  const Json::Value &type_value                = json_value[VIEW_FIELD_TYPE];
  const Json::Value &length_value              = json_value[VIEW_FIELD_LENGTH];

  if (!name_value.isString()) {
    LOG_ERROR("Field name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  if (!original_table_name_value.isString()) {
    LOG_ERROR("original_table_name is not a string. json value=%s", original_table_name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  if (!original_field_name_value.isString()) {
    LOG_ERROR("original_field_name is not a string. json value=%s", original_field_name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  if (!type_value.isString()) {
    LOG_ERROR("Field type is not a string. json value=%s", type_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  if (!length_value.isInt()) {
    LOG_ERROR("Length is not an integer. json value=%s", length_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  string   name                = name_value.asString();
  string   original_table_name = original_table_name_value.asString();
  string   original_field_name = original_field_name_value.asString();
  AttrType type                = attr_type_from_string(type_value.asCString());
  int      length              = length_value.asInt();
  return field.init(name, original_table_name, original_field_name, type, length);
}

RC View::create(Db *db, const char *path, const char *name, const char *base_dir, const string &select_sql)
{
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

  unique_ptr<SelectStmt> select_stmt = nullptr;
  if (OB_FAIL(rc = create_select_stmt(db, select_sql, select_stmt))) {
    return rc;
  }

  vector<ViewFieldMeta> field_metas;
  for (const auto &expr : select_stmt->query_expressions()) {
    field_metas.emplace_back(*expr);
  }

  db_         = db;
  name_       = name;
  select_sql_ = select_sql;
  field_metas_.swap(field_metas);

  fstream fs;
  fs.open(path, ios_base::out | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }
  if (serialize(fs) < 0) {
    LOG_ERROR("Failed to dump view meta to file: %s. sys err=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_WRITE;
  }
  fs.close();

  LOG_INFO("Successfully create view %s:%s", base_dir, name);
  return rc;
}

RC View::create(Db *db, const char *path, const char *name, const char *base_dir, const string &select_sql, const vector<string>& attr_names) {
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

  unique_ptr<SelectStmt> select_stmt = nullptr;
  if (OB_FAIL(rc = create_select_stmt(db, select_sql, select_stmt))) {
    return rc;
  }

  vector<ViewFieldMeta> field_metas;
  for (const auto &expr : select_stmt->query_expressions()) {
    field_metas.emplace_back(*expr);
  }

  db_         = db;
  name_       = name;
  select_sql_ = select_sql;
  field_metas_.swap(field_metas);

  if (attr_names.size() != field_metas_.size()) {
    return RC::SCHEMA_FIELD_MISSING;
  }
  for (size_t i = 0; i < field_metas_.size(); ++i) {
    ViewFieldMeta& field_meta = field_metas_.at(i);
    field_meta.set_name(attr_names.at(i));
  };

  fstream fs;
  fs.open(path, ios_base::out | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }
  if (serialize(fs) < 0) {
    LOG_ERROR("Failed to dump view meta to file: %s. sys err=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_WRITE;
  }
  fs.close();

  LOG_INFO("Successfully create view %s:%s", base_dir, name);
  return RC::SUCCESS;
}

RC View::open(Db *db, const char *name, const char *base_dir)
{
  RC     rc             = RC::SUCCESS;
  string view_file_path = string(base_dir) + common::FILE_PATH_SPLIT_STR + name;

  fstream fs;
  fs.open(view_file_path, ios_base::in | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open view file for read. file name=%s, errmsg=%s", view_file_path.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }

  if (deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize view. file name=%s", view_file_path.c_str());
    fs.close();
    return RC::INTERNAL;
  }
  fs.close();

  db_ = db;

  /* string  select_sql;
   * fstream fs;
   * fs.open(path, ios_base::out | ios_base::binary);
   * fs >> select_sql_;
   * db_ = db; */

  return rc;
}

int View::serialize(ostream &os) const
{
  Json::Value view_value;
  view_value[VIEW_NAME]       = name_;
  view_value[VIEW_SELECT_SQL] = select_sql_;

  Json::Value fields_value;
  for (const ViewFieldMeta &field_meta : field_metas_) {
    Json::Value field_value;
    field_meta.to_json(field_value);
    fields_value.append(std::move(field_value));
  }

  view_value[VIEW_FIELDS] = std::move(fields_value);

  Json::StreamWriterBuilder builder;
  Json::StreamWriter       *writer = builder.newStreamWriter();

  streampos old_pos = os.tellp();
  writer->write(view_value, &os);
  int ret = (int)(os.tellp() - old_pos);

  delete writer;
  return ret;
}

int View::deserialize(istream &is)
{
  Json::Value             view_value;
  Json::CharReaderBuilder builder;
  string                  errors;

  streampos old_pos = is.tellg();
  if (!Json::parseFromStream(builder, is, &view_value, &errors)) {
    LOG_ERROR("Failed to deserialize table meta. error=%s", errors.c_str());
    return -1;
  }

  const Json::Value &view_name_value = view_value[VIEW_NAME];
  if (!view_name_value.isString()) {
    LOG_ERROR("Invalid view name. json value=%s", view_name_value.toStyledString().c_str());
    return -1;
  }
  string view_name = view_name_value.asString();

  const Json::Value &select_sql_value = view_value[VIEW_SELECT_SQL];
  if (!select_sql_value.isString()) {
    LOG_ERROR("Invalid select sql. json value=%s", select_sql_value.toStyledString().c_str());
    return -1;
  }
  string select_sql = select_sql_value.asString();

  const Json::Value &fields_value = view_value[VIEW_FIELDS];
  if (!fields_value.isArray() || fields_value.size() <= 0) {
    LOG_ERROR("Invalid view meta. fields is not array, json value=%s", fields_value.toStyledString().c_str());
    return -1;
  }

  RC                    rc        = RC::SUCCESS;
  int                   field_num = fields_value.size();
  vector<ViewFieldMeta> field_metas(field_num);
  for (int i = 0; i < field_num; i++) {
    ViewFieldMeta &field_meta = field_metas[i];

    const Json::Value &field_value = fields_value[i];
    rc                             = ViewFieldMeta::from_json(field_value, field_meta);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to deserialize view. table name =%s", view_name.c_str());
      return -1;
    }
  }

  name_       = view_name;
  select_sql_ = select_sql;
  field_metas_.swap(field_metas);

  return (int)(is.tellg() - old_pos);
}

int View::get_serial_size() const { return -1; }

void View::to_string(string &output) const {}

const ViewFieldMeta *View::field_meta(const string &name) const
{
  for (const auto &field_meta : field_metas_) {
    if (field_meta.name() == name) {
      return &field_meta;
    }
  }
  return nullptr;
}


RC View::gen_physical_plan(Session *session, unique_ptr<PhysicalOperator> &oper) {
  RC rc = RC::SUCCESS;
  Db *db = session->get_current_db();
  unique_ptr<SelectStmt> select_stmt;
  if (OB_FAIL(rc = create_select_stmt(db, select_sql_, select_stmt))) {
    LOG_WARN("failed to create select stmt in view");
    return rc;
  }

  LogicalPlanGenerator logical_oper_gen{};
  unique_ptr<LogicalOperator> logical_oper;
  if (OB_FAIL(rc = logical_oper_gen.create(select_stmt.get(), logical_oper))) {
    LOG_WARN("failed to create logical operator in view");
    return rc;
  }

  PhysicalPlanGenerator physical_oper_gen{};
  unique_ptr<PhysicalOperator> physical_oper;
  if (OB_FAIL(rc = physical_oper_gen.create(*logical_oper, physical_oper, session))) {
    LOG_WARN("failed to create physical operator in view");
    return rc;
  }

  ASSERT(physical_oper->type() == PhysicalOperatorType::PROJECT, "view's top physical plan must be PROJECT");

  ProjectPhysicalOperator *proj_oper = static_cast<ProjectPhysicalOperator*>(physical_oper.get());
  ViewProjectPhysicalOperator* view_proj_oper = new ViewProjectPhysicalOperator(proj_oper->expressions(), *this);
  view_proj_oper->children().swap(proj_oper->children());
  oper.reset(view_proj_oper);

  return rc;
}


bool View::insertable() {
  for (const auto& field_meta: field_metas_) {
    if (field_meta.original_field_name().empty()) {
      return false;
    }
  }
  return true;
}

RC View::create_select_stmt(Db *db, const string &select_sql, unique_ptr<SelectStmt> &select_stmt)
{
  RC              rc = RC::SUCCESS;
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

  Stmt *stmt = nullptr;
  if (OB_FAIL(rc = Stmt::create_stmt(db, *sql_node, stmt))) {
    return rc;
  }
  select_stmt.reset(static_cast<SelectStmt *>(stmt));

  return rc;
}
