#include "sql/operator/vector_index_scan_physical_operator.h"
#include "storage/index/index.h"

string VectorIndexScanPhysicalOperator::param() const
{
  return string(index_->index_meta().name()) + " ON " + table_->name();
}

RC VectorIndexScanPhysicalOperator::open(Trx *trx)
{
  if (limit_ < 0) {
    return RC::INVALID_ARGUMENT;
  }
  if (limit_ == 0) {
    return RC::SUCCESS;
  }
  rids_ = index_->ann_search(*base_vector_.get_vector(), limit_);
  idx_  = 0;
  tuple_.set_schema(table_, table_->table_meta().field_metas(), table_ref_name_);
  trx_ = trx;
  return RC::SUCCESS;
}

RC VectorIndexScanPhysicalOperator::next()
{
  if (idx_ == rids_.size()) {
    return RC::RECORD_EOF;
  }
  RC rc = RC::SUCCESS;
  if (OB_FAIL(rc = table_->get_record(rids_.at(idx_), current_record_))) {
    return rc;
  }
  tuple_.set_record(&current_record_);
  return RC::SUCCESS;
}

RC VectorIndexScanPhysicalOperator::close() { return RC::SUCCESS; }

Tuple *VectorIndexScanPhysicalOperator::current_tuple() { return &tuple_; }
