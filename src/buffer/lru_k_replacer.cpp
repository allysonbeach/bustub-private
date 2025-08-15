//===----------------------------------------------------------------------===//
//
//                         BusTubA
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock<std::mutex> guard(latch_);
  auto frame_id_to_evict = FindEvictFrameWithoutEvict();
  if (frame_id_to_evict.has_value()) {
    node_store_.erase(frame_id_to_evict.value());
    curr_size_--;
    return frame_id_to_evict.value();
  }

  return std::nullopt;
}

auto LRUKReplacer::FindEvictFrameWithoutEvict() -> std::optional<frame_id_t> {
  // std::scoped_lock<std::mutex> guard(latch_);

  std::optional<frame_id_t> frame_id_to_evict = std::nullopt;
  size_t backward_k_distance = 0;

  std::optional<frame_id_t> inf_frame_id = std::nullopt;
  size_t oldest_timestamp = std::numeric_limits<size_t>::max();

  for (auto &[frame_id, node] : node_store_) {
    if (!node.IsEvictable()) {
      continue;
    }
    auto k_time = node.GetKHistory(k_);
    if (!k_time.has_value()) {
      // This means that there are less than k values in the history
      size_t history_oldest_time = node.GetOldestHistory();
      if (history_oldest_time < oldest_timestamp) {
        oldest_timestamp = history_oldest_time;
        inf_frame_id = node.GetFrameId();
        if (inf_frame_id.has_value()) {
        }
      }
    } else {
      size_t temp_backward_k_distance = current_timestamp_ - k_time.value();
      if (temp_backward_k_distance > backward_k_distance) {
        backward_k_distance = temp_backward_k_distance;
        frame_id_to_evict = node.GetFrameId();
      }
    }
  }

  // Could not find a k distance. Will choose the frame id with the oldest timestamp
  if (inf_frame_id.has_value()) {
    frame_id_to_evict = inf_frame_id;
  }
  if (frame_id_to_evict.has_value()) {
    return frame_id_to_evict.value();
  }

  return std::nullopt;
}

/**
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> guard(latch_);
  // Check if frame is valid
  // TODO(abeach): do we record access only when reading or only when writing? or any time for either
  // Check if the frame id is valid
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("LRUKReplacer.RecordAccess: Invalid frame_id (greater than replacer_size_)");
  }
  // Get the node from the node store for this frame id. If frame id does not exist in node store, it returns
  // default-construct node. Checks if this frame exists in the store
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // Does not exist, so initalize new node
    node_store_.emplace(frame_id, LRUKNode(k_, frame_id));
    it = node_store_.find(frame_id);
  }
  LRUKNode &curr_node = it->second;
  curr_node.AddAccess(current_timestamp_);
  // Now increment timestamp for next access
  current_timestamp_++;
}

/**
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // TODO(abeach): lock_guard vs scoped_guard
  std::scoped_lock<std::mutex> guard(latch_);
  // Check if the frame id is valid
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("SetEvictable: Invalid frame_id (greater than replacer_size_)");
  }
  // Check that it exists first
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // Frame is not in the node store, so there is nothing to modify
    return;
  }
  LRUKNode &curr_node = it->second;
  if (curr_node.IsEvictable() && !set_evictable) {
    // Should set to set_evictable or False?
    curr_node.SetEvictable(set_evictable);
    curr_size_--;
  } else if (!curr_node.IsEvictable() && set_evictable) {
    curr_node.SetEvictable(set_evictable);
    curr_size_++;
  }
}

/**
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> guard(latch_);
  // Check if the frame id is valid
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("LRUKReplacer.Remove: Invalid frame_id (greater than replacer_size_)");
  }
  // Check that it exists first
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // Frame is not in the node store, so there is nothing to modify
    return;
  }
  LRUKNode &curr_node = it->second;
  if (!curr_node.IsEvictable()) {
    throw std::runtime_error("LRUKReplacer.Remove: Cannot remove non-evictable frame");
  }
  node_store_.erase(it);
  curr_size_--;
}

/**
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
