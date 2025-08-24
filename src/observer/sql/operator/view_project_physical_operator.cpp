#include "sql/operator/view_project_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

ViewProjectPhysicalOperator::ViewProjectPhysicalOperator(
    vector<unique_ptr<Expression>> &&expressions, const View& view)
    : expressions_(std::move(expressions))
{
  const auto& view_field_metas = view.field_metas();
  vector<TupleCellSpec> specs;
  for (auto& field_meta: view_field_metas) {
    specs.push_back(TupleCellSpec{view.name().c_str(), field_meta.name().c_str()});
  } 
  tuple_.set_names(specs);
}

RC ViewProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ViewProjectPhysicalOperator::next()
{
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  return children_[0]->next();
}

RC ViewProjectPhysicalOperator::close() { 
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS; 
}

Tuple *ViewProjectPhysicalOperator::current_tuple()
{
  Tuple        *child_tuple = children_[0]->current_tuple();
  RC            rc          = RC::SUCCESS;
  vector<Value> values;
  for (const auto &expr : expressions_) {
    Value tmp;
    rc = expr->get_value(*child_tuple, tmp);
    ASSERT(OB_SUCC(rc), "view_project_physical_operator's get_value must be success");
    values.push_back(tmp);
  }
  tuple_.set_cells(values);
  return &tuple_;
}
