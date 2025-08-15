//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // Assume that frame_ and replacer_ are non null
  // Acquire shread lock to read data
  frame_->rwlatch_.lock_shared();
  // frame_->pin_count_++;
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  // std::cerr << "Move constructing ReadPageGuard (from = " << static_cast<const void *>(&that)
  //         << ", to = " << static_cast<const void *>(this) << ", frame = " << frame_.get() << ")\n";
  // Invalidate that object
  that.page_id_ = 0;  // figure out what is an invalid value
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // Release current resources (empty this, so we can move that to this)
    Drop();

    // Move from that to this
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;

    // Invalidate thatobject
    that.page_id_ = 0;  // figure out what is an invalid value
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
    that.is_valid_ = false;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "ReadPageGuard::GetPageId tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_ && frame_ != nullptr, "ReadPageGuard::GetData tried to use an invalid read guard");
  // std::cerr << "Frame ptr = " << static_cast<const void *>(frame_.get()) << "\n";
  return frame_ != nullptr ? frame_->GetData() : nullptr;  // frame_->GetData()
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "ReadPageGuard::IsDirty tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 */
void ReadPageGuard::Flush() {
  BUSTUB_ASSERT(frame_ != nullptr, "Frame is null in ReadPageGuard::Flush");
  // latch the frame to write back to disk
  // std::unique_lock<std::shared_mutex> guard(frame_->rwlatch_);
  // TODO(abeach): change from scoped to unique lock

  std::promise<bool> p;
  std::future<bool> f = p.get_future();
  // Create a disk request and writing page from memory to disk
  DiskRequest r = {true, frame_->GetDataMut(), page_id_, std::move(p)};
  disk_scheduler_->Schedule(std::move(r));

  // Check for thread timeout
  if (f.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    throw std::runtime_error("ReadPageGuard::Flush timeout on page_id " + std::to_string(page_id_));
  }
  bool success = f.get();
  frame_->is_dirty_ = false;
  BUSTUB_ASSERT(success, "ReadPageGuard::Flush failed to flush to disk");
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * Can eventually use unique_lock when I want to increase the performance, for now/ease, just use scoped_lock
 */
void ReadPageGuard::Drop() {
  // std::cerr << "ReadPageGuard::Drop called (is_valid_ = " << is_valid_
  //         << ", frame = " << static_cast<const void *>(frame_.get()) << ")\n";
  if (!is_valid_) {
    // std::cerr << "Drop skipped (is_valid_ = false)\n";
    return;
  }

  // TODO(abeach): should we acquire BPM's latch to update metadata??
  // if (IsDirty()) {
  //   Flush();
  // }

  // TODO(abeach): should I check if the pin count is greater than 0 - can we have a negative pin count???
  frame_->pin_count_.fetch_sub(1);
  // Release the frame's latch since we will not be reading any more data
  frame_->rwlatch_.unlock_shared();

  if (frame_->pin_count_.load() == 0) {
    std::scoped_lock<std::mutex> lock(*bpm_latch_);  // TODO(abeach): scoped lock should work fine here since it will
                                                     // only lock in this if statement --- I THINK???
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
  is_valid_ = false;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() {
  // std::cerr << "Destroying ReadPageGuard (frame = " << static_cast<const void *>(frame_.get())
  //           << ", is_valid_ = " << is_valid_ << ")\n";
  // std::cerr << "Destroying ReadPageGuard at: " << static_cast<const void *>(this)
  //           << ", frame = " << static_cast<const void *>(frame_.get())
  //           << ", is_valid_ = " << is_valid_ << "\n";
  Drop();
}

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // Use lock to ensure that only one access at a time (exclusivity)
  frame_->rwlatch_.lock();
  // replacer_->RecordAccess(frame_->frame_id_);
  // replacer_->SetEvictable(frame_->frame_id_, false);
  // frame_->pin_count_++;
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  // Invalidate that object
  that.page_id_ = 0;  // figure out what is an invalid value
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    // Release current resources (empty this, so we can move that to this)
    Drop();

    // Move from that to this
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;

    // Invalidate thatobject
    that.page_id_ = 0;  // figure out what is an invalid value
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
    that.is_valid_ = false;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "WritePageGuard::GetPageId tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "WritePageGuard::GetData tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "WritePageGuard::GetDataMut tried to use an invalid write guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "WritePageGuard::IsDirty tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 */
void WritePageGuard::Flush() {
  // Null Check
  BUSTUB_ASSERT(frame_ != nullptr, "Frame is null in WritePageGuard::Flush");

  std::promise<bool> p;
  std::future<bool> f = p.get_future();

  // Create a disk request and writing page from memory to disk
  DiskRequest r = {true, frame_->GetDataMut(), page_id_, std::move(p)};
  disk_scheduler_->Schedule(std::move(r));

  // Check for thread timeout
  if (f.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    throw std::runtime_error("WritePageGuard::Flush timeout on page_id " + std::to_string(page_id_));
  }

  // Wait until finished writing out
  bool success = f.get();
  frame_->is_dirty_ = false;
  BUSTUB_ASSERT(success, "WritePageGuard::Flush: failed to flush to disk");
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 */
void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  // Check if pin count is greater than 1, pin count has to be 1 to be dropped by the WritePageGuard
  // if (frame_->pin_count_ > 1) {
  // Cannot drop since frame is being used
  // return;
  // This return caused a deadlock for some reason
  // std::cerr << "WritePageGuard.Drop(): the frame's pin count is this " << frame_->pin_count_.load() << std::endl;
  // TODO (Jenn): soooo my bug is that this pin_count_ is 2, not 1..... which is weird, but without the if statement it
  // passed the baby tests, so I dunno
  // }
  // Decrement the pin count or set to 0, not sure which is best
  frame_->pin_count_.fetch_sub(1);
  // Release the frame's latch, since we are done reading or writing data
  frame_->rwlatch_.unlock();

  // Acquire the BPM's latch so we can update the metadata
  std::scoped_lock<std::mutex> guard(*bpm_latch_);

  // if (IsDirty()) {
  //   Flush();
  // }
  replacer_->SetEvictable(frame_->frame_id_, true);
  is_valid_ = false;
  // TODO(abeach): will set disk_scheduler_ etc... set to nullptr
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() {
  // std::cerr << "Destroying WritePageGuard at: " << static_cast<const void *>(this)
  //           << ", frame = " << static_cast<const void *>(frame_.get())
  //           << ", is_valid_ = " << is_valid_ << "\n";
  Drop();
}

}  // namespace bustub
