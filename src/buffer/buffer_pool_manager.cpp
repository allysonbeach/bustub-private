//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  next_page_id_.fetch_add(1);
  return next_page_id_.load() - 1;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  // Case 1: page is not in the page table, so not in memory. Remove from disk
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // Page id is not in the page table, thereofre it is not in memory, but could be on disk
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  auto frame_opt = FindFrameHeader(it->second);
  // If the page is in the page table, then it should find the frame
  BUSTUB_ASSERT(frame_opt.has_value(), "BufferPoolManager::DeletePage: Frame header not found for existing page");
  std::shared_ptr<FrameHeader> frame = frame_opt.value();
  if (frame->pin_count_.load() > 0) {
    // It is currently pinned and cannot be deleted from memory and/or disk
    return false;
  }

  // remove from the replacer
  replacer_->Remove(frame->frame_id_);
  // reset all of the things
  page_table_.erase(page_id);
  frame->page_id_ = INVALID_PAGE_ID;
  frame->Reset();
  free_frames_.push_back(frame->frame_id_);

  // then deallocate page from disk
  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::unique_lock<std::mutex> lock(*bpm_latch_);

  // Case 1:
  if (page_table_.find(page_id) != page_table_.end()) {
    // Then page is already in BPM memory
    auto it = page_table_.find(page_id);
    auto frame_opt = FindFrameHeader(it->second);  // it -> second get's the value, frame_id
    std::shared_ptr<FrameHeader> frame = frame_opt.value();
    BUSTUB_ASSERT(frame, "BufferPoolManager::CheckedWritePage FrameHeader should exist after inserting frame_id");

    // Create the Write Guard
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame->frame_id_);
    replacer_->SetEvictable(frame->frame_id_, false);
    lock.unlock();
    // TODO(abeach): move the pin_count_ incrementing outside of the constructor to here - since
    // TODO(abeach): unlock BPM's latch after you update the pin count
    WritePageGuard guard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
    return guard;
  }

  // Case 2:
  // Memory is available, need to do I/O
  if (!free_frames_.empty()) {
    // Pick a free frame from free_frames
    frame_id_t new_frame_id = free_frames_.front();
    free_frames_.remove(new_frame_id);
    auto frame_opt = FindFrameHeader(new_frame_id);

    BUSTUB_ASSERT(frame_opt.has_value(),
                  "BufferPoolManager::CheckedWritePage FrameHeader should exist after retrieving from free frames");

    std::shared_ptr<FrameHeader> frame = frame_opt.value();

    // Schedule the I/O to read the page data from disk to memory
    ScheduleIO(false, frame->GetDataMut(), page_id);

    // Update the page table and the frame header
    page_table_[page_id] = new_frame_id;
    frame->page_id_ = page_id;
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame->frame_id_);
    replacer_->SetEvictable(frame->frame_id_, false);

    // Release the BPM latch to prevent deadlocking
    lock.unlock();
    // Construct the read page guard
    WritePageGuard guard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
    return guard;
  }

  // Case 3:
  // Need to evict a frame from replacer to free up one to use
  auto evicted_frame_id = replacer_->FindEvictFrameWithoutEvict();
  if (!evicted_frame_id.has_value()) {
    // There are no pages to evict, this can happen - should it error out?
    lock.unlock();
    return std::nullopt;
  }

  // Now can use this frame to create a ReadPageGuard
  auto frame_opt = FindFrameHeader(evicted_frame_id.value());
  BUSTUB_ASSERT(frame_opt.has_value(), "FrameHeader should exist after retrieving frame from replacer Evict");

  // Check the pin count, cannot evict if pin count is greater than 0
  if (frame_opt.value()->pin_count_.load() > 0) {
    // Must return because you cannot evict if the pin count is greater than 0
    lock.unlock();
    return std::nullopt;
  }
  // Now that we checked the pin count, we can evict/remove from replacer
  replacer_->Remove(evicted_frame_id.value());

  std::shared_ptr<FrameHeader> frame = frame_opt.value();
  BUSTUB_ASSERT(frame->page_id_.has_value(),
                "BufferPoolManager::CheckedWritePage the evicted FrameHeader should have a page associated with it");

  // Flush the old evicted frame from memory to disk if dirty
  if (frame->is_dirty_) {
    // Schedule the I/O to write the page data from memory to disk
    ScheduleIO(true, frame->GetDataMut(), frame->page_id_.value());
    frame->is_dirty_ = false;
  }

  // Handle reseting frame and reassigning new page info
  BUSTUB_ASSERT(page_table_.count(frame->page_id_.value()) > 0,
                "BufferPoolManager::CheckedWritePage Evicted page not found in page table");
  page_table_.erase(frame->page_id_.value());
  frame->Reset();
  frame->page_id_ = page_id;
  page_table_[page_id] = frame->frame_id_;
  frame->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame->frame_id_);
  replacer_->SetEvictable(frame->frame_id_, false);

  // Schedule the I/O to read the page data from disk to memory
  ScheduleIO(false, frame->GetDataMut(), page_id);

  // Release the BPM's latch to prevent deadlocking
  lock.unlock();
  // Create the read guard
  WritePageGuard guard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  return guard;
  // // Don't keep the BPM's latch?
  // lock.unlock();
  // return std::nullopt;
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::unique_lock<std::mutex> lock(*bpm_latch_);

  // Case 1:
  if (page_table_.find(page_id) != page_table_.end()) {
    // Then page is already in BPM memory
    auto it = page_table_.find(page_id);
    auto frame = FindFrameHeader(it->second);  // it -> second get's the value, frame_id
    BUSTUB_ASSERT(frame.has_value(),
                  "BufferPoolManager::CheckedReadPage FrameHeader should exist after inserting frame_id");

    frame.value()->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame.value()->frame_id_);
    replacer_->SetEvictable(frame.value()->frame_id_, false);
    // Release BPM's latch before acquiring the frame's latch
    lock.unlock();
    ReadPageGuard guard(page_id, frame.value(), replacer_, bpm_latch_, disk_scheduler_);
    return guard;
  }

  // Case 2:
  // Memory is available, need to do I/O
  if (!free_frames_.empty()) {
    // Pick a free frame from free_frames
    frame_id_t new_frame_id = free_frames_.front();
    free_frames_.remove(new_frame_id);
    auto frame_opt = FindFrameHeader(new_frame_id);

    BUSTUB_ASSERT(frame_opt.has_value(),
                  "BufferPoolManager::CheckedReadPage FrameHeader should exist after retrieving from free frames");

    std::shared_ptr<FrameHeader> frame = frame_opt.value();

    // Schedule the I/O to read the page data from disk to memory
    ScheduleIO(false, frame->GetDataMut(), page_id);

    // Update the page table and the frame header
    page_table_[page_id] = new_frame_id;
    frame->page_id_ = page_id;
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame->frame_id_);
    replacer_->SetEvictable(frame->frame_id_, false);

    // Release BPM's latch before acquiring the frame's latch
    lock.unlock();
    // Construct the read page guard
    ReadPageGuard guard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
    return guard;
  }

  // Case 3:
  // Need to evict a frame from replacer to free up one to use

  // Find the frame we want to evict/remove from the replacer
  auto evicted_frame_id = replacer_->FindEvictFrameWithoutEvict();
  if (!evicted_frame_id.has_value()) {
    // There are no pages to evict, this can happen - should it error out?
    lock.unlock();
    return std::nullopt;
  }

  // Now can use this frame to create a ReadPageGuard
  auto frame_opt = FindFrameHeader(evicted_frame_id.value());
  BUSTUB_ASSERT(frame_opt.has_value(),
                "BufferPoolManager::CheckedReadPage FrameHeader should exist after retrieving frame from replacer");
  if (frame_opt.value()->pin_count_.load() > 0) {
    // Must return because you cannot evict if the pin count is greater than 0
    lock.unlock();
    return std::nullopt;
  }
  // Now that we checked the pin count, we can evict/remove from replacer
  replacer_->Remove(evicted_frame_id.value());

  std::shared_ptr<FrameHeader> frame = frame_opt.value();
  if (!frame->page_id_.has_value()) {
    // Error, the evicted FrameHeader should have a page associated with it
    lock.unlock();
    return std::nullopt;
  }

  // Flush the old evicted frame from memory to disk if dirty
  if (frame->is_dirty_) {
    // Schedule the I/O to write the page data from memory to disk
    ScheduleIO(true, frame->GetDataMut(), frame->page_id_.value());
    frame->is_dirty_ = false;
  }

  // Handle reseting frame and reassigning new page info
  BUSTUB_ASSERT(page_table_.count(frame->page_id_.value()) > 0,
                "BufferPoolManager::CheckedReadPage Evicted page not found in page table");
  page_table_.erase(frame->page_id_.value());
  frame->Reset();
  frame->page_id_ = page_id;
  page_table_[page_id] = frame->frame_id_;
  frame->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame->frame_id_);
  replacer_->SetEvictable(frame->frame_id_, false);

  // Schedule the I/O to read the page data from disk to memory
  ScheduleIO(false, frame->GetDataMut(), page_id);

  // Release BPM's latch before acquiring the frame's latch
  lock.unlock();
  // Create the read guard
  ReadPageGuard guard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  return guard;

  // lock.unlock();
  // return std::nullopt;
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  return std::move(guard_opt.value());
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  // return std::move(guard_opt.value());
  BUSTUB_ASSERT(guard_opt.has_value(), "CheckedReadPage failed");
  ReadPageGuard guard = std::move(*guard_opt);  // move from a named object, not a temp
  // std::cerr << "Returning guard from ReadPage with frame = " << guard.frame_.get() << "\n";
  return guard;
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(abeach): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  UNIMPLEMENTED("TODO(abeach): Add implementation.");
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(abeach): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { UNIMPLEMENTED("TODO(abeach): Add implementation."); }

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(abeach): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() { UNIMPLEMENTED("TODO(abeach): Add implementation."); }

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(abeach): Add implementation
 */
void BufferPoolManager::FlushAllPages() { UNIMPLEMENTED("TODO(abeach): Add implementation."); }

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(abeach): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock<std::mutex> guard(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // Page exists in the page table
    auto frame_opt = FindFrameHeader(it->second);  // it -> second get's the value, frame_id
    BUSTUB_ASSERT(frame_opt.has_value(),
                  "BufferPoolManager::GetPinCount FrameHeader should exist after retrieving frame page table");

    return frame_opt.value()->pin_count_.load();
  }
  return std::nullopt;
}

/**
 * @brief Helper Function - Find Frame Header
 * Retrieves the frame header from the Buffer Pool Manager's list of frame via the frame id
 *
 * @param frame_id the frame ID that we want to find
 * @return std::optional<std::shared_ptr<FrameHeader>>
 */
auto BufferPoolManager::FindFrameHeader(frame_id_t frame_id) -> std::optional<std::shared_ptr<FrameHeader>> {
  for (std::shared_ptr<FrameHeader> frame_header : frames_) {
    if (frame_header->frame_id_ == frame_id) {
      return frame_header;
    }
  }
  // Did not find the frame in BPM's frames, error
  return std::nullopt;
}

/**
 * @brief Helper Function - Schedule I/O
 * Uses the disk scheduler to schedule the I/O needed
 *
 * @param is_write flag if we should write to disk (from memory) or read from disk (to memory)
 * @param data is a pointer to the data to be written or read from disk
 * @param page_id is the page id that is going to be written/read from disk
 * @return std::optional<std::shared_ptr<FrameHeader>>
 */
// NOLINTNEXTLINE(readability-non-const-parameter)
void BufferPoolManager::ScheduleIO(bool is_write, char *data,
                                   page_id_t page_id)  // NOLINT(readability-non-const-parameter)
{
  std::promise<bool> p;
  auto f = p.get_future();
  disk_scheduler_->Schedule({is_write, data, page_id, std::move(p)});
  f.get();
}

}  // namespace bustub
