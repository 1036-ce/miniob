#pragma once

#include "sql/expr/tuple.h"
#include "sql/operator/physical_operator.h"
#include "storage/index/ivfflat_index.h"
#include "storage/record/record_manager.h"

/**
 * @brief 索引扫描物理算子
 * @ingroup PhysicalOperator
 */
class VectorIndexScanPhysicalOperator : public PhysicalOperator
{
public:
  VectorIndexScanPhysicalOperator(Table *table, IvfflatIndex *index, const Value &base_vector, int limit)
      : table_(table), index_(index), base_vector_(base_vector), limit_(limit), table_ref_name_(table->name())
  {}

  virtual ~VectorIndexScanPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::VECTOR_INDEX_SCAN; }

  string param() const override;

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  RC filter(RowTuple &tuple, bool &result);

private:
  Table        *table_ = nullptr;
  IvfflatIndex *index_ = nullptr;
  Value         base_vector_;
  int           limit_;
  string        table_ref_name_;

  RowTuple    tuple_;
  Record      current_record_;
  vector<RID> rids_;
  size_t      idx_;
};
