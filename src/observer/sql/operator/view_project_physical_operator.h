#pragma once

#include "sql/operator/physical_operator.h"
#include "storage/view/view.h"

/**
 * @brief 选择/投影物理算子
 * @ingroup PhysicalOperator
 */
class ViewProjectPhysicalOperator : public PhysicalOperator
{
public:
  ViewProjectPhysicalOperator(vector<unique_ptr<Expression>> &&expressions, const View& view);

  virtual ~ViewProjectPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::VIEW_PROJECT; }
  OpType               get_op_type() const override { return OpType::PROJECTION; }

  virtual double calculate_cost(
      LogicalProperty *prop, const vector<LogicalProperty *> &child_log_props, CostModel *cm) override
  {
    return (cm->cpu_op()) * prop->get_card();
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  int cell_num() const { return tuple_.cell_num(); }

  Tuple *current_tuple() override;
private:
  vector<unique_ptr<Expression>>          expressions_;
  ValueListTuple tuple_;
};
