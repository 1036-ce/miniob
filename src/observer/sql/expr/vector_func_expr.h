#include "sql/expr/expression.h"
#include "common/type/vector_type.h"

class UnboundVectorFuncExpr : public Expression
{
public:
  UnboundVectorFuncExpr(const char *func_name, Expression *left_child, Expression *right_child)
      : func_name_(func_name), left_child_(left_child), right_child_(right_child)
  {}
  UnboundVectorFuncExpr(
      const char *distance_name, unique_ptr<Expression> left_child, unique_ptr<Expression> right_child)
      : func_name_(distance_name), left_child_(std::move(left_child)), right_child_(std::move(right_child))
  {}
  virtual ~UnboundVectorFuncExpr() = default;

  ExprType type() const override { return ExprType::UNBOUND_VECTOR_FUNC; }

  unique_ptr<Expression> copy() const override
  {
    return make_unique<UnboundVectorFuncExpr>(func_name_.c_str(), left_child_->copy(), right_child_->copy());
  }

  const char *function_name() const { return func_name_.c_str(); }

  unique_ptr<Expression> &left_child() { return left_child_; }
  unique_ptr<Expression> &right_child() { return right_child_; }

  RC       get_value(const Tuple &tuple, Value &value) const override { return RC::INTERNAL; }
  AttrType value_type() const override { return AttrType::FLOATS; }

private:
  string                 func_name_;
  unique_ptr<Expression> left_child_;
  unique_ptr<Expression> right_child_;
};

class VectorFuncExpr : public Expression
{
public:
  VectorFuncExpr(VectorFuncType type, Expression *left_child, Expression *right_child)
      : func_type_(type), left_child_(left_child), right_child_(right_child)
  {}
  VectorFuncExpr(VectorFuncType type, unique_ptr<Expression> left_child, unique_ptr<Expression> right_child)
      : func_type_(type), left_child_(std::move(left_child)), right_child_(std::move(right_child))
  {}
  virtual ~VectorFuncExpr() = default;

  unique_ptr<Expression> copy() const override
  {
    return make_unique<VectorFuncExpr>(func_type_, left_child_->copy(), right_child_->copy());
  }

  RC get_value(const Tuple &tuple, Value &value) const override;
  RC calc_value(const Value &left_value, const Value &right_value, Value &value) const;
  RC get_column(Chunk &chunk, Column &column) override;

  ExprType type() const override { return ExprType::VECTOR_FUNC; }
  AttrType value_type() const override { return AttrType::FLOATS; }
  int      value_length() const override { return 4; }

  RC to_computable();

  VectorFuncType                    func_type() const { return func_type_; }
  unique_ptr<Expression> &left_child() { return left_child_; }
  unique_ptr<Expression> &right_child() { return right_child_; }

public:
  static RC type_from_string(const char *type_str, VectorFuncType &type);

private:
  VectorFuncType                   func_type_;
  unique_ptr<Expression> left_child_;
  unique_ptr<Expression> right_child_;
};
