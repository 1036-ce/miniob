#pragma once

#include "sql/stmt/stmt.h"
#include "storage/field/field_meta.h"

struct CreateVectorIndexSqlNode;
class Table;
class FieldMeta;

/**
 * @brief 创建向量索引的语句
 * @ingroup Statement
 */
class CreateVectorIndexStmt : public Stmt
{
public:
  CreateVectorIndexStmt(
      Table *table, const FieldMeta &field_meta, const string &index_name, const unordered_map<string, string> &params)
      : table_(table), field_meta_(field_meta), index_name_(index_name), params_(params)
  {}

  virtual ~CreateVectorIndexStmt() = default;

  StmtType type() const override { return StmtType::CREATE_VECTOR_INDEX; }

  Table                               *table() const { return table_; }
  const FieldMeta                     &field_meta() const { return field_meta_; }
  const string                        &index_name() const { return index_name_; }
  const unordered_map<string, string> &params() const { return params_; }

public:
  static RC create(Db *db, const CreateVectorIndexSqlNode &create_index, Stmt *&stmt);

private:
  static RC gen_params(const vector<unique_ptr<Param>>& param_list, unordered_map<string, string>& params);

  Table                        *table_ = nullptr;
  FieldMeta                     field_meta_;
  string                        index_name_;
  unordered_map<string, string> params_;
};
