#include "storage/index/ivfflat_index.h"
#include "sql/expr/vector_func_expr.h"
#include <random>

static constexpr const string IVFFLAT_TYPE           = "ivfflat";
static constexpr const string IVFFLAT_PARAM_TYPE     = "type";
static constexpr const string IVFFLAT_PARAM_DISTANCE = "distance";
static constexpr const string IVFFLAT_PARAM_LISTS    = "lists";
static constexpr const string IVFFLAT_PARAM_PROBES   = "probes";

RC IvfflatIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta,
    const unordered_map<string, string> &params)
{
  if (params.size() != 4) {
    return RC::INVALID_ARGUMENT;
  }
  if (!params.contains(IVFFLAT_PARAM_TYPE) || params.at(IVFFLAT_PARAM_TYPE) != IVFFLAT_TYPE) {
    return RC::INVALID_ARGUMENT;
  }
  if (!params.contains(IVFFLAT_PARAM_DISTANCE) || !params.contains(IVFFLAT_PARAM_LISTS) ||
      !params.contains(IVFFLAT_PARAM_PROBES)) {
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;
  if (OB_FAIL(rc = VectorType::type_from_string(params.at(IVFFLAT_PARAM_DISTANCE).c_str(), func_type_))) {
    return rc;
  }
  if (OB_FAIL(rc = str2int(params.at(IVFFLAT_PARAM_LISTS), lists_))) {
    return rc;
  }
  if (OB_FAIL(rc = str2int(params.at(IVFFLAT_PARAM_PROBES), probes_))) {
    return rc;
  }

  table_      = table;
  field_meta_ = field_meta;
  return RC::SUCCESS;
}

RC IvfflatIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta,
    const unordered_map<string, string> &params)
{
  return RC::SUCCESS;
}

vector<RID> IvfflatIndex::ann_search(const vector<float> &base_vector, size_t limit) { 
  Value base_val;
  base_val.set_vector(base_vector);
  auto center_comp = [this, &base_val](size_t lhs, size_t rhs) -> bool {
    float lhs_dist;
    float rhs_dist;
    distance(base_val, centers_.at(lhs), lhs_dist);
    distance(base_val, centers_.at(rhs), rhs_dist);
    return lhs_dist < rhs_dist;
  };
  auto value_comp = [this, &base_val](size_t lhs, size_t rhs) -> bool {
    float lhs_dist;
    float rhs_dist;
    distance(base_val, values_.at(lhs), lhs_dist);
    distance(base_val, values_.at(rhs), rhs_dist);
    return lhs_dist < rhs_dist;
  };

  std::priority_queue<size_t, vector<size_t>, decltype(center_comp)> center_idx_pq{center_comp};
  for (int i = 0; i < lists_; ++i) {
    if (center_idx_pq.size() < static_cast<size_t>(probes_)) {
      center_idx_pq.push(i);
    }
    else {
      if (center_comp(i, center_idx_pq.top())) {
        center_idx_pq.pop();
        center_idx_pq.push(i);
      }
    }
  }

  std::priority_queue<size_t, vector<size_t>, decltype(value_comp)> rid_idx_pq{value_comp};
  while (!center_idx_pq.empty()) {
    int center_idx = center_idx_pq.top();
    center_idx_pq.pop();
    for (size_t rid_idx: clusters_.at(center_idx)) {
      if (rid_idx_pq.size() < limit) {
        rid_idx_pq.push(rid_idx);
      }
      else {
        if (value_comp(rid_idx, rid_idx_pq.top())) {
          rid_idx_pq.pop();
          rid_idx_pq.push(rid_idx);
        }
      }
    }
  }

  vector<RID> ret;
  while (!rid_idx_pq.empty()) {
    size_t rid_idx = rid_idx_pq.top();
    rid_idx_pq.pop();
    ret.push_back(rids_.at(rid_idx));
  }
  std::reverse(ret.begin(), ret.end());

  return ret;
}

RC IvfflatIndex::insert_entry(const Record &record)
{
  RC    rc = RC::SUCCESS;
  Value val;
  if (OB_FAIL(get_value_from_record(record, val))) {
    return rc;
  }
  rids_.push_back(record.rid());
  values_.push_back(val);

  if (!trained_) {
    return RC::SUCCESS;
  }

  float dist;
  int   center_idx;
  if (OB_FAIL(rc = get_nearest_center_distance(val, dist, center_idx))) {
    return rc;
  }
  clusters_.at(center_idx).push_back(values_.size() - 1);
  return RC::SUCCESS;
}

RC IvfflatIndex::delete_entry(const Record &record)
{
  for (RID &rid : rids_) {
    if (record.rid() == rid) {
      rid = RID::invalid_rid();
    }
  }
  return RC::SUCCESS;
}

RC IvfflatIndex::kmeans_init()
{
  RC                               rc = RC::SUCCESS;
  std::random_device               rd;
  std::mt19937                     engine(rd());
  std::uniform_real_distribution<> distr(0, 1);

  int idx = static_cast<int>(distr(engine) * values_.size());
  centers_.push_back(values_.at(idx));
  clusters_.emplace_back();

  for (int i = 1; i < lists_; ++i) {
    vector<float> dists;
    for (const Value &value : values_) {
      float dist;
      int   center_idx;
      if (OB_FAIL(rc = get_nearest_center_distance(value, dist, center_idx))) {
        return rc;
      }
      dists.push_back(dist);
    }
    idx = choose(dists, distr(engine));
    centers_.push_back(values_.at(idx));
    clusters_.emplace_back();
  }

  return RC::SUCCESS;
}

RC IvfflatIndex::kmeans_train()
{
  RC rc = RC::SUCCESS;

  if (OB_FAIL(rc = kmeans_init())) {
    return rc;
  }

  int max_iter_count = 10;
  while (max_iter_count--) {
    vector<Value> new_centers(lists_);
    for (int i = 0; i < lists_; ++i) {
      clusters_.at(i).clear();
      new_centers.at(i).set_vector(field_meta_.real_len() / sizeof(float), 0.0);
    }

    for (size_t i = 0; i < values_.size(); ++i) {
      const Value &value = values_.at(i);
      float        dist;
      int          center_idx;
      if (OB_FAIL(rc = get_nearest_center_distance(value, dist, center_idx))) {
        return rc;
      }
      clusters_.at(center_idx).push_back(i);
      if (OB_FAIL(rc = Value::add(new_centers.at(center_idx), value, new_centers.at(center_idx)))) {
        return rc;
      }
    }

    // get new centers
    for (int i = 0; i < lists_; ++i) {
      Value &new_center    = new_centers.at(i);
      auto   vec       = new_center.get_vector();
      float  inv_count = 1.0 / static_cast<float>(clusters_.at(i).size());
      for (auto &val : *vec) {
        val = val * inv_count;
      }
    }

    bool has_converged = true;
    for (int i = 0; i < lists_; ++i) {
      const auto &new_center = new_centers.at(i);
      auto &old_center = centers_.at(i);
      float       dist;
      if (OB_FAIL(rc = distance(new_center, old_center, dist))) {
        return rc;
      }
      if (std::abs(dist) > 1e-3) {
        has_converged = false;
      }
      old_center = new_center;
    }

    if (has_converged) {
      LOG_INFO("train finish because converged");
      trained_ = true;
      return RC::SUCCESS;
    }
  }

  LOG_INFO("train done because touch max_iter_count");
  trained_ = true;
  return RC::SUCCESS;
}

float IvfflatIndex::get_match_score(const TableGetLogicalOperator &oper)
{
  auto &orderby = oper.orderby();
  if (!orderby || oper.limit() == -1) {
    return 0.0;
  }
  if (orderby->type() != ExprType::VECTOR_FUNC) {
    return 0.0;
  }

  auto vec_func_expr = static_cast<VectorFuncExpr *>(orderby.get());
  if (vec_func_expr->func_type() != func_type_) {
    return 0.0;
  }

  if (vec_func_expr->left_child()->type() == ExprType::TABLE_FIELD) {
    auto left_child = static_cast<TableFieldExpr *>(vec_func_expr->left_child().get());
    if (strcasecmp(left_child->field_name(), field_meta_.name()) != 0) {
      return 0.0;
    }
  } else if (vec_func_expr->right_child()->type() == ExprType::TABLE_FIELD) {
    auto right_child = static_cast<TableFieldExpr *>(vec_func_expr->right_child().get());
    if (strcasecmp(right_child->field_name(), field_meta_.name()) != 0) {
      return 0.0;
    }
  } else {
    return 0.0;
  }

  return 1.0;
}

unique_ptr<PhysicalOperator> IvfflatIndex::gen_physical_oper(const TableGetLogicalOperator &oper) 
{
  return nullptr;
}

RC IvfflatIndex::str2int(const string &str, int &val)
{
  const char *pos    = str.data();
  bool        is_neg = false;
  if (*pos == '-') {
    is_neg = true;
    ++pos;
  }

  val = 0;
  while (*pos >= '0' && *pos <= '9') {
    val = val * 10 + (*pos - '0');
    ++pos;
  }

  if (*pos == '\0') {
    val = is_neg ? -val : val;
    return RC::SUCCESS;
  }
  return RC::INVALID_ARGUMENT;
}

RC IvfflatIndex::get_value_from_record(const Record &record, Value &val)
{
  val.set_type(AttrType::VECTORS);
  val.set_data(record.data() + field_meta_.offset(), field_meta_.real_len());
  return RC::SUCCESS;
}

RC IvfflatIndex::distance(const Value &left, const Value &right, float &result)
{
  RC    rc = RC::SUCCESS;
  Value tmp;
  switch (func_type_) {
    case VectorFuncType::L2_DISTANCE: {
      rc = VectorType{}.l2_distance(left, right, tmp);
    } break;
    case VectorFuncType::COSINE_DISTANCE: {
      rc = VectorType{}.cosine_distance(left, right, tmp);
    } break;
    case VectorFuncType::INNER_PRODUCT: {
      rc = VectorType{}.inner_product(left, right, tmp);
    } break;
    default: return RC::INVALID_ARGUMENT;
  }
  result = tmp.get_float();
  return rc;
}

RC IvfflatIndex::get_nearest_center_distance(const Value &val, float &dist, int &center_idx)
{
  RC    rc = RC::SUCCESS;
  float tmp;
  dist       = std::numeric_limits<float>::max();
  center_idx = -1;

  for (size_t i = 0; i < centers_.size(); ++i) {
    const auto &center = centers_.at(i);
    if (OB_FAIL(rc = distance(center, val, tmp))) {
      return rc;
    }
    if (tmp < dist) {
      dist       = tmp;
      center_idx = i;
    }
  }
  return RC::SUCCESS;
}

int IvfflatIndex::choose(const vector<float> &dists, float rand)
{
  float inv_sum_dist2 =
      std::accumulate(dists.begin(), dists.end(), 0, [](float init, float elem) { return init + elem * elem; });
  inv_sum_dist2 = 1.0 / std::max(inv_sum_dist2, static_cast<float>(EPSILON));

  float val = 0;
  for (size_t i = 0; i < dists.size(); ++i) {
    float dist2 = dists.at(i) * dists.at(i);
    val         = val + dist2 * inv_sum_dist2;
    if (val > rand) {
      return i;
    }
  }
  return dists.size() - 1;
}
