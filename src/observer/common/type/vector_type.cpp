#include "common/lang/comparator.h"
#include "common/type/vector_type.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include <cmath>

int VectorType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::VECTORS && right.attr_type() == AttrType::VECTORS, "left and right must be vector");
  vector<float> *left_vec   = left.get_vector();
  vector<float> *right_vec  = right.get_vector();
  int            left_size  = left_vec->size();
  int            right_size = right_vec->size();

  int pos = 0;
  while (pos < left_size && pos < right_size) {
    float left_val  = left_vec->at(pos);
    float right_val = right_vec->at(pos);
    auto  res       = common::compare_float((void *)&left_val, (void *)&right_val);
    if (res != 0) {
      return res;
    }
    ++pos;
  }
  if (left_size == right_size) {
    return 0;
  }
  if (left_size > right_size) {
    return 1;
  }
  return -1;
}

RC VectorType::add(const Value &left, const Value &right, Value &result) const
{
  if (left.attr_type() != AttrType::VECTORS || right.attr_type() != AttrType::VECTORS) {
    return RC::INVALID_ARGUMENT;
  }
  vector<float> *left_vec  = left.get_vector();
  vector<float> *right_vec = right.get_vector();
  if (left_vec->size() != right_vec->size()) {
    return RC::INVALID_ARGUMENT;
  }

  int           size = left_vec->size();
  vector<float> result_vec;
  result_vec.reserve(size);
  for (int i = 0; i < size; ++i) {
    result_vec.push_back(left_vec->at(i) + right_vec->at(i));
  }
  result.set_vector(std::move(result_vec));
  return RC::SUCCESS;
}

RC VectorType::subtract(const Value &left, const Value &right, Value &result) const
{
  if (left.attr_type() != AttrType::VECTORS || right.attr_type() != AttrType::VECTORS) {
    return RC::INVALID_ARGUMENT;
  }
  vector<float> *left_vec  = left.get_vector();
  vector<float> *right_vec = right.get_vector();
  if (left_vec->size() != right_vec->size()) {
    return RC::INVALID_ARGUMENT;
  }

  int           size = left_vec->size();
  vector<float> result_vec;
  result_vec.reserve(size);
  for (int i = 0; i < size; ++i) {
    result_vec.push_back(left_vec->at(i) - right_vec->at(i));
  }
  result.set_vector(std::move(result_vec));
  return RC::SUCCESS;
}

RC VectorType::multiply(const Value &left, const Value &right, Value &result) const
{
  if (left.attr_type() != AttrType::VECTORS || right.attr_type() != AttrType::VECTORS) {
    return RC::INVALID_ARGUMENT;
  }
  vector<float> *left_vec  = left.get_vector();
  vector<float> *right_vec = right.get_vector();
  if (left_vec->size() != right_vec->size()) {
    return RC::INVALID_ARGUMENT;
  }

  int           size = left_vec->size();
  vector<float> result_vec;
  result_vec.reserve(size);
  for (int i = 0; i < size; ++i) {
    result_vec.push_back(left_vec->at(i) * right_vec->at(i));
  }
  result.set_vector(std::move(result_vec));
  return RC::SUCCESS;
}

RC VectorType::cast_to(const Value &val, AttrType type, Value &result) const {
  RC rc;
  switch (type) {
    case AttrType::CHARS: {
      rc = CharType().set_value_from_str(result, val.to_string());
    } break;
    default: LOG_WARN("unsupported type %d", type); return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  return rc;
}

int VectorType::cast_cost(AttrType type) {
  switch (type) {
    case AttrType::VECTORS: 
      return 0;
    case AttrType::CHARS:
      return 2;
    default:
      return INT32_MAX;
  }
}

RC VectorType::set_value_from_str(Value &val, const string &data) const
{
  RC rc = RC::SUCCESS;
  vector<float> vec;
  const char   *pos = data.c_str();

  while (std::isblank(*pos)) {
    ++pos;
  }
  if (*pos != '[') {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  ++pos;
  while (*pos != ']' && *pos != '\0') {
    if (std::isblank(*pos) || *pos == ',') {
      ++pos;
      continue;
    }
    float val = 0;
    if (OB_FAIL(rc = str2float(pos, val))) {
      return rc;
    }
    vec.push_back(val);
  }

  // 没有']' 类型不完全
  if (*pos == '\0') {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  val.set_vector(vec);
  return RC::SUCCESS;
}

RC VectorType::str2float(const char* &pos, float &val) const
{
  bool is_neg = false;
  if (*pos == '-') {
    is_neg = true;
    ++pos;
  }

  val = 0;
  while (*pos >= '0' && *pos <= '9') {
    val = val * 10 + (*pos - '0');
    ++pos;
  }

  if (*pos == '.') {
    ++pos;
    float base = 0.1;
    while (*pos >= '0' && *pos <= '9') {
      val = val + ((*pos - '0') * base);
      base *= 0.1;
      ++pos;
    }
  }

  if (std::isblank(*pos) || *pos == ',' || *pos == ']') {
    val = is_neg ? -val : val;
    return RC::SUCCESS;
  }
  else {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

RC VectorType::to_string(const Value &val, string &result) const
{
  vector<float> *vec  = val.get_vector();
  int            size = vec->size();

  stringstream ss;
  ss << "[";
  for (int i = 0; i < size; ++i) {
    ss << common::double_to_str(vec->at(i));
    if (i < size - 1) {
      ss << ",";
    }
  }
  ss << "]";
  result = ss.str();
  return RC::SUCCESS;
}

RC VectorType::l2_distance(const Value &left, const Value &right, Value &result) const {
  if (left.attr_type() != AttrType::VECTORS || right.attr_type() != AttrType::VECTORS) {
    return RC::INVALID_ARGUMENT;
  }
  vector<float> *left_vec  = left.get_vector();
  vector<float> *right_vec = right.get_vector();
  if (left_vec->size() != right_vec->size()) {
    return RC::INVALID_ARGUMENT;
  }

  int           size = left_vec->size();
  float val = 0;
  float tmp;
  for (int i = 0; i < size; ++i) {
    tmp = left_vec->at(i) - right_vec->at(i);
    val += tmp * tmp;
  }
  val = std::sqrt(val);
  result.set_float(val);
  return RC::SUCCESS;
}

RC VectorType::cosine_distance(const Value &left, const Value &right, Value &result) const {
  if (left.attr_type() != AttrType::VECTORS || right.attr_type() != AttrType::VECTORS) {
    return RC::INVALID_ARGUMENT;
  }
  vector<float> *left_vec  = left.get_vector();
  vector<float> *right_vec = right.get_vector();
  if (left_vec->size() != right_vec->size()) {
    return RC::INVALID_ARGUMENT;
  }

  int           size = left_vec->size();
  float val = 0;
  float up = 0;
  float left_norm = 0;
  float right_norm = 0;
  for (int i = 0; i < size; ++i) {
    up += left_vec->at(i) * right_vec->at(i);
    left_norm += left_vec->at(i) * left_vec->at(i);
    right_norm += right_vec->at(i) * right_vec->at(i);
  }
  left_norm = std::sqrt(left_norm);
  right_norm = std::sqrt(right_norm);

  if (left_norm == 0 || right_norm == 0) {
    result.set_float(0);
    result.set_null(true);
  }
  else {
    val = 1 - up/(left_norm * right_norm);
    result.set_float(val);
  }
  return RC::SUCCESS;
}

RC VectorType::inner_product(const Value &left, const Value &right, Value &result) const {
  if (left.attr_type() != AttrType::VECTORS || right.attr_type() != AttrType::VECTORS) {
    return RC::INVALID_ARGUMENT;
  }
  vector<float> *left_vec  = left.get_vector();
  vector<float> *right_vec = right.get_vector();
  if (left_vec->size() != right_vec->size()) {
    return RC::INVALID_ARGUMENT;
  }

  int           size = left_vec->size();
  float val = 0;
  for (int i = 0; i < size; ++i) {
    val += left_vec->at(i) * right_vec->at(i);
  }
  result.set_float(val);
  return RC::SUCCESS;
}

RC VectorType::type_from_string(const char *type_str, VectorFuncType &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "l2_distance")) {
    type = VectorFuncType::L2_DISTANCE;
  } else if (0 == strcasecmp(type_str, "cosine_distance")) {
    type = VectorFuncType::COSINE_DISTANCE;
  } else if (0 == strcasecmp(type_str, "inner_product")) {
    type = VectorFuncType::INNER_PRODUCT;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}
