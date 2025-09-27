#include "storage/index/ivfflat_index.h"
#include "sql/expr/vector_func_expr.h"
#include "sql/operator/vector_index_scan_physical_operator.h"
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
  index_meta_ = index_meta;
  return RC::SUCCESS;
}

RC IvfflatIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta,
    const unordered_map<string, string> &params)
{
  return RC::SUCCESS;
}

/* vector<RID> IvfflatIndex::ann_search(const vector<float> &base_vector, int limit)
 * {
 *   if (need_retrain()) {
 *     RC rc = kmeans_train();
 *     ASSERT(OB_SUCC(rc), "retrain must be success");
 *   }
 * 
 *   Value base_val;
 *   base_val.set_vector(base_vector);
 * 
 *   std::priority_queue<SearchEntry> center_idx_pq;
 *   for (size_t i = 0; i < centers_.size(); ++i) {
 *     float dist;
 *     distance_relative(base_val, centers_.at(i), dist);
 *     if (center_idx_pq.size() < static_cast<size_t>(probes_)) {
 *       center_idx_pq.emplace(i, dist);
 *     } else {
 *       if (dist < center_idx_pq.top().distance) {
 *         center_idx_pq.pop();
 *         center_idx_pq.emplace(i, dist);
 *       }
 *     }
 *   }
 * 
 *   std::priority_queue<SearchEntry> rid_idx_pq;
 *   while (!center_idx_pq.empty()) {
 *     size_t center_idx = center_idx_pq.top().idx;
 *     center_idx_pq.pop();
 *     for (size_t rid_idx : clusters_.at(center_idx)) {
 *       float dist;
 *       distance_relative(base_val, values_.at(rid_idx), dist);
 *       if (rid_idx_pq.size() < static_cast<size_t>(limit)) {
 *         rid_idx_pq.emplace(rid_idx, dist);
 *       } else {
 *         if (dist < rid_idx_pq.top().distance) {
 *           rid_idx_pq.pop();
 *           rid_idx_pq.emplace(rid_idx, dist);
 *         }
 *       }
 *     }
 *   }
 * 
 *   vector<RID> ret;
 *   while (!rid_idx_pq.empty()) {
 *     size_t rid_idx = rid_idx_pq.top().idx;
 *     rid_idx_pq.pop();
 *     ret.push_back(rids_.at(rid_idx));
 *   }
 *   std::reverse(ret.begin(), ret.end());
 * 
 *   return ret;
 * } */

vector<RID> IvfflatIndex::ann_search(const vector<float> &base_vector, int limit)
{
  if (need_retrain()) {
    RC rc = kmeans_train();
    ASSERT(OB_SUCC(rc), "retrain must be success");
  }

  Value base_val;
  base_val.set_vector(base_vector);

  std::priority_queue<SearchEntry> center_idx_pq;
  for (size_t i = 0; i < centers_.size(); ++i) {
    float dist;
    if (center_idx_pq.size() < static_cast<size_t>(probes_)) {
      distance_relative(base_val, centers_.at(i), dist);
      center_idx_pq.emplace(i, dist);
    } else {
      more_near_distance(base_val, centers_.at(i), center_idx_pq.top().distance, dist);
      if (dist != -1) {
        center_idx_pq.pop();
        center_idx_pq.emplace(i, dist);
      }
      /* if (dist < center_idx_pq.top().distance) {
       *   center_idx_pq.pop();
       *   center_idx_pq.emplace(i, dist);
       * } */
    }
  }

  std::priority_queue<SearchEntry> rid_idx_pq;
  while (!center_idx_pq.empty()) {
    size_t center_idx = center_idx_pq.top().idx;
    center_idx_pq.pop();
    for (size_t rid_idx : clusters_.at(center_idx)) {
      float dist;
      if (rid_idx_pq.size() < static_cast<size_t>(limit)) {
        distance_relative(base_val, values_.at(rid_idx), dist);
        rid_idx_pq.emplace(rid_idx, dist);
      } else {
        more_near_distance(base_val, values_.at(rid_idx), rid_idx_pq.top().distance, dist);
        if (dist != -1) {
          rid_idx_pq.pop();
          rid_idx_pq.emplace(rid_idx, dist);
        }
      }
    }
  }

  vector<RID> ret;
  while (!rid_idx_pq.empty()) {
    size_t rid_idx = rid_idx_pq.top().idx;
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
  ++insert_num_after_train_;
  return RC::SUCCESS;
}

RC IvfflatIndex::delete_entry(const Record &record)
{
  for (RID &rid : rids_) {
    if (record.rid() == rid) {
      rid = RID::invalid_rid();
    }
  }
  ++delete_num_after_train_;
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

  vector<float> nearest_dists;
  nearest_dists.reserve(values_.size());
  for (const Value &value : values_) {
    float dist;
    if (OB_FAIL(rc = distance(value, centers_.at(0), dist))) {
      return rc;
    }
    nearest_dists.push_back(dist);
  }

  for (int i = 1; i < lists_; ++i) {
    idx = choose(nearest_dists, distr(engine));
    centers_.push_back(values_.at(idx));
    clusters_.emplace_back();

    for (size_t j = 0; j < values_.size(); ++j) {
      const Value &value    = values_.at(j);
      float        old_dist = nearest_dists.at(j);
      float        new_dist;
      if (OB_FAIL(rc = distance(value, centers_.back(), new_dist))) {
        return rc;
      }
      if (new_dist < old_dist) {
        nearest_dists.at(j) = new_dist;
      }
    }
  }

  return RC::SUCCESS;
}

RC IvfflatIndex::kmeans_train()
{
  RC rc = RC::SUCCESS;

  centers_.clear();
  clusters_.clear();
  remove_deleted();

  if (values_.size() <= static_cast<size_t>(lists_)) {
    centers_ = values_;
    for (size_t i = 0; i < values_.size(); ++i) {
      clusters_.push_back({i});
    }
    return RC::SUCCESS;
  }

  LOG_INFO("init begin");
  if (OB_FAIL(rc = kmeans_init())) {
    return rc;
  }
  LOG_INFO("init done");

  int max_iter_count = 1;
  while (max_iter_count--) {
    LOG_INFO("iterate begin");
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
      Value &new_center = new_centers.at(i);
      auto   vec        = new_center.get_vector();
      float  inv_count  = 1.0 / static_cast<float>(clusters_.at(i).size());
      for (auto &val : *vec) {
        val = val * inv_count;
      }
    }

    bool has_converged = true;
    for (int i = 0; i < lists_; ++i) {
      const auto &new_center = new_centers.at(i);
      auto       &old_center = centers_.at(i);
      float       dist;
      if (OB_FAIL(rc = distance(new_center, old_center, dist))) {
        return rc;
      }
      if (std::abs(dist) > 0.1) {
        has_converged = false;
      }
      old_center = new_center;
    }

    if (has_converged) {
      LOG_INFO("train finish because converged");
      trained_ = true;
      return RC::SUCCESS;
    }
    LOG_INFO("iterate end");
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
  auto &orderby = oper.orderby();
  if (orderby == nullptr || orderby->type() != ExprType::VECTOR_FUNC) {
    return nullptr;
  }

  auto vec_func_expr = static_cast<VectorFuncExpr *>(orderby.get());
  if (vec_func_expr->func_type() != func_type_) {
    return nullptr;
  }

  ValueExpr *val_expr;
  if (vec_func_expr->left_child()->type() == ExprType::VALUE) {
    val_expr = static_cast<ValueExpr *>(vec_func_expr->left_child().get());
  } else if (vec_func_expr->right_child()->type() == ExprType::VALUE) {
    val_expr = static_cast<ValueExpr *>(vec_func_expr->right_child().get());
  } else {
    return nullptr;
  }

  Value base_vector = val_expr->get_value();

  return make_unique<VectorIndexScanPhysicalOperator>(oper.table(), this, base_vector, oper.limit());
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

RC IvfflatIndex::distance_relative(const Value &left, const Value &right, float &result)
{
  RC    rc = RC::SUCCESS;
  Value tmp;
  switch (func_type_) {
    case VectorFuncType::L2_DISTANCE: {
      rc = VectorType{}.l2_distance_square(left, right, tmp);
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

void IvfflatIndex::more_near_distance(const Value &left, const Value &right, float upper_bound, float &result) 
{
  if (func_type_ == VectorFuncType::L2_DISTANCE) {
    float *lhs  = left.get_vector()->data();
    float *rhs = right.get_vector()->data();
    size_t size = left.get_vector()->size();
    float ret = 0;
    for (size_t pos = 0; pos < size; ++pos) {
      float tmp = lhs[pos] - rhs[pos];
      ret += tmp * tmp;
      if (ret > upper_bound) {
        result = -1;
        return ;
      }
    }
    result = ret;
    return;
  }
  else {
    distance_relative(left, right, result);
  }
}

RC IvfflatIndex::get_nearest_center_distance(const Value &val, float &dist, int &center_idx)
{
  RC rc      = RC::SUCCESS;
  dist       = std::numeric_limits<float>::max();
  center_idx = -1;

  for (size_t i = 0; i < centers_.size(); ++i) {
    const auto &center = centers_.at(i);
    float       tmp    = 0;
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

void IvfflatIndex::remove_deleted()
{
  size_t size = rids_.size();
  size_t pre  = 0;
  size_t cur  = 0;

  while (pre < size && rids_.at(pre) == RID::invalid_rid()) {
    ++pre;
    ++cur;
  }

  while (cur < size) {
    if (rids_.at(cur) != RID::invalid_rid()) {
      std::swap(rids_.at(pre), rids_.at(cur));
      std::swap(values_.at(pre), values_.at(cur));
      ++pre;
    }
    ++cur;
  }

  while (pre < size) {
    rids_.pop_back();
    values_.pop_back();
    ++pre;
  }
}

bool IvfflatIndex::need_retrain()
{
  float change_num = insert_num_after_train_ + delete_num_after_train_;
  float total_size = static_cast<float>(std::max<int>(values_.size(), 1));
  if (change_num / total_size > 0.2) {
    return true;
  }
  return false;
}
