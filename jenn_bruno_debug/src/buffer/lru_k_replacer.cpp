//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <ctime>
#include <optional>
#include <stdexcept>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : num_frames_(num_frames), k_(k) {}

/**
 * @brief Find and evicts the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID of the evicted frame, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock<std::mutex> guard(latch_);
  auto victim_opt = FindVictimFrame();
  if (!victim_opt.has_value()) {
    return std::nullopt;
  }
  auto victim_frame_id = victim_opt.value();
  auto ptr_to_map_with_victim = FindMapWithFrame(victim_frame_id);
  BUSTUB_ENSURE(ptr_to_map_with_victim != nullptr, "Can't find victim frame in map")
  if (ptr_to_map_with_victim == &frames_with_k_access_) {
    frames_with_k_access_.erase(victim_frame_id);
  } else {
    frames_less_than_k_access_.erase(victim_frame_id);
  };
  curr_size_ -= 1;
  return victim_frame_id;
}

/**
 * @brief Finds the victim frame when multiple frames have inf backward k-distance. Note that
 * no locking is needed here since Evict() locks the mutex! DOES NOT EVICT!!!
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame can be successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::FindVictimHelperOverallTimestamp() -> std::optional<frame_id_t> {
  std::optional<frame_id_t> frame_id_with_min_ts = std::nullopt;
  std::optional<time_t> min_ts = std::nullopt;
  for (auto &[frame_id, frame] : frames_less_than_k_access_) {
    if (!frame.is_evictable_) {
      continue;
    }
    if (!frame_id_with_min_ts) {
      frame_id_with_min_ts = frame_id;
      min_ts = frame.access_times_.front();
      continue;
    }
    if (frame.access_times_.front() < min_ts) {
      frame_id_with_min_ts = frame_id;
      min_ts = frame.access_times_.front();
    }
  }
  return frame_id_with_min_ts;
}

/**
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction. DOES NOT EVICT!!!
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame that can be successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
auto LRUKReplacer::FindVictimFrame() -> std::optional<frame_id_t> {
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  // if there's at least one evictable frame in frames_less_than_k_access: return FindVictimHelper_OverallTimestamp
  if (!frames_less_than_k_access_.empty()) {
    for (auto &[frame_id, frame] : frames_less_than_k_access_) {
      if (frame.is_evictable_) {
        return FindVictimHelperOverallTimestamp();
      }
    }
  }
  // return based on largest backward k-distance (aka front of list is the oldest)
  std::optional<frame_id_t> frame_id_with_min_ts = std::nullopt;
  std::optional<size_t> min_ts = std::nullopt;
  for (auto &[frame_id, frame] : frames_with_k_access_) {
    if (!frame.is_evictable_) {
      continue;
    }
    if (!frame_id_with_min_ts) {
      frame_id_with_min_ts = frame_id;
      min_ts = frame.access_times_.front();
      continue;
    }
    if (frame.access_times_.front() < min_ts) {
      frame_id_with_min_ts = frame_id;
      min_ts = frame.access_times_.front();
    }
  }
  return frame_id_with_min_ts;
}

auto LRUKReplacer::FindMapWithFrame(frame_id_t frame_id) -> std::unordered_map<frame_id_t, LRUKNode> * {
  auto it1 = frames_with_k_access_.find(frame_id);
  auto it2 = frames_less_than_k_access_.find(frame_id);
  if (it1 == frames_with_k_access_.end() && it2 == frames_less_than_k_access_.end()) {
    return nullptr;
  }
  if (it1 != frames_with_k_access_.end()) {
    return &frames_with_k_access_;
  }
  return &frames_less_than_k_access_;
}

/**
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
  if (static_cast<size_t>(frame_id) > num_frames_) {
    throw std::runtime_error("Invalid frame ID, larger than max size");
  }
  size_t curr_time = current_timestamp_;
  auto ptr_to_map = FindMapWithFrame(frame_id);
  if (ptr_to_map == nullptr) {
    frames_less_than_k_access_.emplace(frame_id, LRUKNode(frame_id, curr_time));
  } else if (ptr_to_map == &frames_with_k_access_) {
    frames_with_k_access_.at(frame_id).access_times_.pop_front();
    frames_with_k_access_.at(frame_id).access_times_.push_back(curr_time);
  } else if (ptr_to_map == &frames_less_than_k_access_) {
    auto &node = frames_less_than_k_access_.at(frame_id);
    node.access_times_.push_back(curr_time);
    if (node.access_times_.size() == k_) {  // promote to frames_with_k_access_ if it now has k accesses
      auto node_entry = frames_less_than_k_access_.extract(frame_id);
      frames_with_k_access_.insert(std::move(node_entry));
    }
  }
  current_timestamp_ += 1;
}

/**
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
  std::scoped_lock<std::mutex> guard(latch_);
  auto ptr_to_map = FindMapWithFrame(frame_id);
  if (ptr_to_map == nullptr) {
    std::cout << "Frame not in either map; cannot toggle evictability" << '\n';
    return;
  }
  bool orig_state = (*ptr_to_map).at(frame_id).is_evictable_;
  (*ptr_to_map).at(frame_id).is_evictable_ = set_evictable;
  if (!orig_state && set_evictable) {
    curr_size_ += 1;
    return;
  }
  if (orig_state && !set_evictable) {
    curr_size_ -= 1;
    return;
  }
}

/**
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
  auto ptr_to_map = FindMapWithFrame(frame_id);
  if (ptr_to_map == nullptr) {
    return;
  }
  if (!(*ptr_to_map).at(frame_id).is_evictable_) {
    throw std::runtime_error("Tried to remove non-evictable frame");
  }
  ptr_to_map->erase(frame_id);
  curr_size_ -= 1;
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
