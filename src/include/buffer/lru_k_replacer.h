//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::list<size_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};

 public:
  LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

  auto GetK() const -> size_t { return k_; }
  auto GetFrameId() const -> frame_id_t { return fid_; }
  auto IsEvictable() const -> bool { return is_evictable_; }

  void SetEvictable(bool evictable) { is_evictable_ = evictable; }
  void AddAccess(size_t current_timestamp) {
    history_.push_back(current_timestamp);

    if (history_.size() > k_) {
      history_.pop_front();
    }
  }
  auto GetKHistory(size_t k) -> std::optional<size_t> {
    if (k > history_.size()) {
      return std::nullopt;
    }
    auto it = history_.begin();
    std::advance(it, k - 1);
    return *it;
  }
  auto GetOldestHistory() -> size_t {
    if (!history_.empty()) {
      return history_.front();
    }
    return std::numeric_limits<size_t>::max();
  }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(abeach): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  auto FindEvictFrameWithoutEvict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
