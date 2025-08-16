#include "sql/expr/subquery_expression.h"
#include "sql/optimizer/logical_plan_generator.h"

RC SubQueryExpr::build_select_stmt(BinderContext& binder_context)
{
  RC    rc   = RC::SUCCESS;
  Stmt *stmt = nullptr;
  if (sql_node_ == nullptr) {
    return rc;
  }

  BinderContext sub_binder_context;
  sub_binder_context.set_db(binder_context.db());
  for (auto table: binder_context.outer_query_tables()) {
    sub_binder_context.add_outer_table(table);
  }
  for (auto table: binder_context.query_tables()) {
    sub_binder_context.add_outer_table(table);
  }

  if (OB_FAIL(rc = SelectStmt::create(binder_context.db(), sql_node_->selection, stmt, sub_binder_context))) {
    return rc;
  } 
  select_stmt_.reset(static_cast<SelectStmt *>(stmt));

  for (auto table: sub_binder_context.used_outer_tables()) {
    if (binder_context.find_outer_table(table->name()) != nullptr) {
      binder_context.add_used_outer_table(table);
    }
  }

  is_correlated_ = !sub_binder_context.used_outer_tables().empty();
  if (is_correlated_) {
    LOG_DEBUG("a correlated subquery occured");
  }

  return rc;
}

RC SubQueryExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  if (is_correlated_) {
    // physical_oper_ has been opened
    if (OB_FAIL(rc = physical_oper_->next())) {
      if (rc == RC::RECORD_EOF) {
        value.set_null(true);
        return RC::SUCCESS;
      }
      return rc;
    }

    Tuple *subquery_tuple = physical_oper_->current_tuple();
    if (OB_FAIL(rc = subquery_tuple->cell_at(0, value))) {
      return rc;
    }

    rc = physical_oper_->next();
    if (rc != RC::RECORD_EOF) {
      LOG_WARN("subquery return more than one line");
      return rc;
    }

    return RC::SUCCESS;
  }
  else {
    if (values_.size() > 1) {
      LOG_WARN("subquery return more than one line");
      return RC::UNSUPPORTED;
    }
    if (values_.empty()) {
      value.set_null(true);
      return RC::SUCCESS;
    }
    value = values_.front();
    return RC::SUCCESS;
  }
}

RC SubQueryExpr::related_tables(vector<const Table *> &tables) const {
  return RC::UNIMPLEMENTED;
}

RC SubQueryExpr::run_uncorrelated_query(Trx *trx)
{
  RC          rc = RC::SUCCESS;

  if (OB_FAIL(rc = physical_oper_->open(trx))) {
    return rc;
  }

  while (true) {
    rc = physical_oper_->next();
    if (OB_FAIL(rc)) {
      if (rc == RC::RECORD_EOF) {
        break;
      }
      return rc;
    }

    Tuple *tuple = physical_oper_->current_tuple();
    Value  value;
    tuple->cell_at(0, value);
    values_.push_back(value);
  }

  if (OB_FAIL(rc = physical_oper_->close())) {
    return rc;
  }

  return RC::SUCCESS;
}
