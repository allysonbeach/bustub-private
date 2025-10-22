//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include <mutex>
#include "common/macros.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  frame_->rwlatch_.lock_shared();  // waits if a write guard exists.
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * @param that The other page guard (moves from that to this)
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {  // don't have to re-lock (it steals it)
  if (!that.is_valid_) {
    return;
  }
  this->page_id_ = that.page_id_;
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->is_valid_ = true;
  that.page_id_ = -1;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  // std::cerr << "move assignment operator, empty out this" << '\n';
  Drop();  // empty this
  if (!that.is_valid_) {
    return *this;
  }
  // move stuff over... no need to relock
  this->page_id_ = that.page_id_;
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->is_valid_ = true;
  that.page_id_ = -1;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 */
void ReadPageGuard::Drop() {
  // std::cerr << "ReadPG Drop for page_id " << page_id_ << '\n';
  if (!this->is_valid_) {
    return;
  }
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  // std::cerr << "Dropping ReadPageGuard on frame " << frame_->frame_id_
  //           << " with pin_count= " << frame_->pin_count_.load() << '\n';
  BUSTUB_ENSURE(this->frame_->pin_count_.load() > 0,
                "can't drop readpageguard and decr pin count on a frame with pins <= 0")
  this->frame_->pin_count_.fetch_sub(1);
  if (this->frame_->pin_count_.load() == 0) {
    this->replacer_->SetEvictable(frame_->frame_id_, true);
  }
  lock.unlock();
  this->frame_->rwlatch_.unlock_shared();
  this->is_valid_ = false;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  frame_->rwlatch_.lock();  // waits until the page/frame not in use if needed. can only have one writepageguard and
                            // only if there's no other guards.
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // key invariant of RAII is moving transfers the ownership so you don't have to unlock to move
  // don't have to re-lock
  if (!that.is_valid_) {
    return;
  }
  this->page_id_ = that.page_id_;
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->is_valid_ = true;
  that.page_id_ = -1;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  // std::cerr << "move assignment operator, empty out this" << '\n';
  Drop();  // empty out this
  if (!that.is_valid_) {
    return *this;
  }
  // move stuff over... again, don't have to relock
  this->page_id_ = that.page_id_;
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->is_valid_ = true;
  that.page_id_ = -1;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 */
void WritePageGuard::Drop() {
  // std::cerr << "WritePG Drop for page_id " << page_id_ << '\n';
  if (!this->is_valid_) {
    return;
  }
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  // std::cerr << "Dropping WritePageGuard on frame " << frame_->frame_id_
  //           << " with pin_count= " << frame_->pin_count_.load() << '\n';
  // BUSTUB_ENSURE(frame_->pin_count_.load() == 1, "should be only one pin")
  this->frame_->pin_count_.fetch_sub(1);
  if (this->frame_->pin_count_.load() == 0) {
    this->replacer_->SetEvictable(frame_->frame_id_, true);
  }
  lock.unlock();
  this->frame_->rwlatch_.unlock();
  this->is_valid_ = false;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
