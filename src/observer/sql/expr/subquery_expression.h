#pragma once

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "sql/stmt/select_stmt.h"

class SubQueryExpr : public Expression
{
public:
  SubQueryExpr(ParsedSqlNode *sql_node) : sql_node_(sql_node) {}
  virtual ~SubQueryExpr() = default;

  unique_ptr<Expression> copy() const override { return nullptr; }

  // Only called for `val = (subquery)`, `val > (subquery)` and so on.
  RC get_value(const Tuple &tuple, Value &value) const override;

  string   to_string() const override { return ""; }
  ExprType type() const override { return ExprType::SUBQUERY; }
  AttrType value_type() const override { return AttrType::UNDEFINED; }
  RC       related_tables(vector<const Table *> &tables) const override;

  // RC build_select_stmt(Db *db, const vector<Table *> &tables);
  RC build_select_stmt(BinderContext& binder_context);
  RC run_uncorrelated_query(Trx *trx);

  unique_ptr<ParsedSqlNode>    &sql_node() { return sql_node_; }
  unique_ptr<SelectStmt>       &select_stmt() { return select_stmt_; }
  unique_ptr<LogicalOperator>  &logical_oper() { return logical_oper_; }
  unique_ptr<PhysicalOperator> &physical_oper() { return physical_oper_; }

  /* void set_logical_oper(LogicalOperator *logical_oper) { logical_oper_ = logical_oper; }
   * void set_physical_oper(PhysicalOperator *physical_oper) { physical_oper_ = physical_oper; } */
  void set_logical_oper(unique_ptr<LogicalOperator> logical_oper) { logical_oper_ = std::move(logical_oper); }
  void set_physical_oper(unique_ptr<PhysicalOperator> physical_oper) { physical_oper_ = std::move(physical_oper); }

  bool                 is_correlated() { return is_correlated_; }
  const vector<Value> &values() { return values_; }

private:
  unique_ptr<ParsedSqlNode>    sql_node_;
  unique_ptr<SelectStmt>       select_stmt_;
  unique_ptr<LogicalOperator>  logical_oper_;
  unique_ptr<PhysicalOperator> physical_oper_;
  /* LogicalOperator          *logical_oper_;
   * PhysicalOperator         *physical_oper_; */

  bool          is_correlated_{true};
  vector<Value> values_;  // for uncorrelated subquery
};
