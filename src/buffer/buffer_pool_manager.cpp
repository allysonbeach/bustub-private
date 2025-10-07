//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
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
  // LOG_FUNCTION_CALL();
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
  page_id_ = INVALID_PAGE_ID;
}

void FrameHeader::UpdatePageId(page_id_t new_page_id) { page_id_ = new_page_id; }

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
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
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
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  int new_page_id = next_page_id_.fetch_add(1);
  disk_scheduler_->IncreaseDiskSpace(new_page_id + 1);
  return new_page_id;
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
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  printf("BPM DeletePage page id %d.\n", page_id);
  std::scoped_lock<std::mutex> lock(*bpm_latch_);
  // Case 1: page is not in the page table, so not in memory. Remove from disk
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    printf("BPM::DeletePage page id is NOT in the page table \n");
    // Page id is not in the page table, thereofre it is not in memory, but could be on disk
    // could be a future improvement, disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  auto frame_opt = FindFrameHeader(it->second);
  // If the page is in the page table, then it should find the frame
  // BUSTUB_ENSURE(frame_opt.has_value(), "BufferPoolManager::DeletePage: Frame header not found for existing page\n");
  std::shared_ptr<FrameHeader> frame = frame_opt.value();
  if (frame->pin_count_.load() > 0) {
    // It is currently pinned and cannot be deleted from memory and/or disk
    printf("BPM::DeletePage COULD NOT DELETE frame %d is currently pinned with pin count %lu and with page id %d\n",
           frame->frame_id_, frame->pin_count_.load(), page_id);
    return false;
  }
  // printf("BPM::DeletePage Removing frame %d and page id %d from the replacer, page table and reset\n",
  // frame->frame_id_,
  //        page_id);
  // auto frame_id = page_table_[page_id];
  // remove from the replacer
  // reset all of the things
  page_table_.erase(page_id);
  free_frames_.push_back(frame->frame_id_);
  replacer_->Remove(frame->frame_id_);
  frame->Reset();

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
  // LOG_FUNCTION_CALL();
  printf("BPM CheckedWritePage page id %d.\n", page_id);
  auto frame_header_ptr_opt = GetOrMakeFrameForGuard(page_id, true);
  if (!frame_header_ptr_opt.has_value()) {  // no frame available to load page into, so no new pins were added
    return std::nullopt;
  }
  return WritePageGuard(page_id, frame_header_ptr_opt.value(), replacer_, bpm_latch_);
}

// OLD CHECKEDWRITEPAGE
/*
  // std::unique_lock<std::mutex> lock(*bpm_latch_);
  // Case 1:
  if (page_table_.find(page_id) != page_table_.end()) {
    // Then page is already in BPM memory
    auto it = page_table_.find(page_id);
    auto frame_opt = FindFrameHeader(it->second);  // it -> second get's the value, frame_id
    std::shared_ptr<FrameHeader> frame = frame_opt.value();
    BUSTUB_ENSURE(frame, "BufferPoolManager::CheckedWritePage FrameHeader should exist after inserting frame_id");
    if (frame->needs_to_be_reloaded_) {
      // TODO(abeach): make this into a helper function like LoadPageIntoFrame
      // Schedule the I/O to read the page data from disk to memory
      page_table_[page_id] = frame->frame_id_;
      auto frame_header_ptr = frames_[frame->frame_id_];
      frame_header_ptr->UpdatePageId(page_id);
      ScheduleIO(false, frame->GetDataMut(), page_id);
      frame_header_ptr->needs_to_be_reloaded_ = false;
      // auto frame_header_ptr = LoadPageIntoFrame(page_id, frame -> frame_id_);
    }
    // Create the Write Guard
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame->frame_id_);
    replacer_->SetEvictable(frame->frame_id_, false);
    lock.unlock();
    // TODO(abeach): move the pin_count_ incrementing outside of the constructor to here - since
    // TODO(abeach): unlock BPM's latch after you update the pin count
    WritePageGuard guard(page_id, frame, replacer_, bpm_latch_);
    return guard;
  }

  // Case 2:
  // Memory is available, need to do I/O
  if (!free_frames_.empty()) {
    // Pick a free frame from free_frames
    frame_id_t new_frame_id = free_frames_.front();
    free_frames_.remove(new_frame_id);
    auto frame_opt = FindFrameHeader(new_frame_id);

    BUSTUB_ENSURE(frame_opt.has_value(),
                  "BufferPoolManager::CheckedWritePage FrameHeader should exist after retrieving from free frames");

    std::shared_ptr<FrameHeader> frame = frame_opt.value();

    // Schedule the I/O to read the page data from disk to memory
    ScheduleIO(false, frame->GetDataMut(), page_id);

    // Update the page table and the frame header
    page_table_[page_id] = new_frame_id;
    frame->UpdatePageId(page_id);
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame->frame_id_);
    replacer_->SetEvictable(frame->frame_id_, false);

    // Release the BPM latch to prevent deadlocking
    lock.unlock();
    // Construct the read page guard
    WritePageGuard guard(page_id, frame, replacer_, bpm_latch_);
    return guard;
  }

  // Case 3:
  // Need to evict a frame from replacer to free up one to use
  auto evicted_frame_id = replacer_->FindEvictFrameWithoutEvict();
  if (!evicted_frame_id.has_value()) {
    // There are no pages to evict, this can happen - should it error out?
    lock.unlock();
    printf("BPM::CheckedWritePage FindEvictFrameWithoutEvict but nothing is evictable.\n");
    return std::nullopt;
  }

  // Now can use this frame to create a ReadPageGuard
  auto frame_opt = FindFrameHeader(evicted_frame_id.value());
  BUSTUB_ENSURE(frame_opt.has_value(), "FrameHeader should exist after retrieving frame from replacer Evict");

  // Check the pin count, cannot evict if pin count is greater than 0
  if (frame_opt.value()->pin_count_.load() > 0) {
    // Must return because you cannot evict if the pin count is greater than 0
    lock.unlock();
    printf("BPM::CheckedWritePage tried to Evict but frame %d is pinned.\n", frame_opt.value() -> frame_id_);
    return std::nullopt;
  }
  printf("BPM::CheckedWritePage, evicting frame id %d holding page id %d.\n", frame_opt.value()->frame_id_,
         frame_opt.value()->page_id_);
  // Now that we checked the pin count, we can evict/remove from replacer
  replacer_->Remove(evicted_frame_id.value());

  std::shared_ptr<FrameHeader> frame = frame_opt.value();
  BUSTUB_ENSURE(frame->page_id_,
                "BufferPoolManager::CheckedWritePage the evicted FrameHeader should have a page associated with it");
  BUSTUB_ENSURE(frame->page_id_ != INVALID_PAGE_ID, "invalid page id from frame header ptr");
  // Flush the old evicted frame from memory to disk if dirty
  if (frame->is_dirty_) {
    // Schedule the I/O to write the page data from memory to disk
    ScheduleIO(true, frame->GetDataMut(), frame->page_id_);
    frame->is_dirty_ = false;
  }

  // Handle reseting frame and reassigning new page info
  BUSTUB_ENSURE(page_table_.count(frame->page_id_) > 0,
                "BufferPoolManager::CheckedWritePage Evicted page not found in page table");
  page_table_.erase(frame->page_id_);
  frame->Reset();
  // Load Page into Frame
  frame->UpdatePageId(page_id);
  page_table_[page_id] = frame->frame_id_;

  frame->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame->frame_id_);
  replacer_->SetEvictable(frame->frame_id_, false);

  // Schedule the I/O to read the page data from disk to memory
  ScheduleIO(false, frame->GetDataMut(), page_id);

  // Release the BPM's latch to prevent deadlocking
  lock.unlock();
  // Create the read guard
  WritePageGuard guard(page_id, frame, replacer_, bpm_latch_);
  return guard; */
// // Don't keep the BPM's latch?
// lock.unlock();
// return std::nullopt;

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
  // LOG_FUNCTION_CALL();
  printf("BPM CheckedReadPage page id %d.\n", page_id);
  auto frame_header_ptr_opt = GetOrMakeFrameForGuard(page_id, false);
  if (!frame_header_ptr_opt.has_value()) {  // no frame available to load page into, so no new pins were added
    return std::nullopt;
  }
  return ReadPageGuard(page_id, frame_header_ptr_opt.value(), replacer_, bpm_latch_);
}

// OLD CHECKED READ PAGE
/* std::unique_lock<std::mutex> lock(*bpm_latch_);

  // Case 1:
  if (page_table_.find(page_id) != page_table_.end()) {
    // Then page is already in BPM memory
    auto it = page_table_.find(page_id);
    auto frame_opt = FindFrameHeader(it->second);  // it -> second get's the value, frame_id
    BUSTUB_ENSURE(frame_opt.has_value(),
                  "BufferPoolManager::CheckedReadPage FrameHeader should exist after inserting frame_id");
    std::shared_ptr<FrameHeader> frame = frame_opt.value();
    if (frame->needs_to_be_reloaded_) {
      // TODO(abeach): make this into a helper function like LoadPageIntoFrame
      // Schedule the I/O to read the page data from disk to memory
      page_table_[page_id] = frame->frame_id_;
      auto frame_header_ptr = frames_[frame->frame_id_];
      frame_header_ptr->UpdatePageId(page_id);
      ScheduleIO(false, frame->GetDataMut(), page_id);
      frame_header_ptr->needs_to_be_reloaded_ = false;
    }
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame->frame_id_);
    replacer_->SetEvictable(frame->frame_id_, false);
    // Release BPM's latch before acquiring the frame's latch
    lock.unlock();
    ReadPageGuard guard(page_id, frame, replacer_, bpm_latch_);
    return guard;
  }

  // Case 2:
  // Memory is available, need to do I/O
  if (!free_frames_.empty()) {
    // Pick a free frame from free_frames
    frame_id_t new_frame_id = free_frames_.front();
    free_frames_.remove(new_frame_id);
    auto frame_opt = FindFrameHeader(new_frame_id);

    BUSTUB_ENSURE(frame_opt.has_value(),
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
    ReadPageGuard guard(page_id, frame, replacer_, bpm_latch_);
    return guard;
  }

  // Case 3:
  // Need to evict a frame from replacer to free up one to use

  // Find the frame we want to evict/remove from the replacer
  auto evicted_frame_id = replacer_->FindEvictFrameWithoutEvict();
  if (!evicted_frame_id.has_value()) {
    // There are no pages to evict, this can happen - should it error out?
    lock.unlock();
    printf("BPM TryToEvictPage but nothing is evictable.\n");
    return std::nullopt;
  }

  // Now can use this frame to create a ReadPageGuard
  auto frame_opt = FindFrameHeader(evicted_frame_id.value());
  BUSTUB_ENSURE(frame_opt.has_value(),
                "BufferPoolManager::CheckedReadPage FrameHeader should exist after retrieving frame from replacer");
  if (frame_opt.value()->pin_count_.load() > 0) {
    // Must return because you cannot evict if the pin count is greater than 0
    lock.unlock();
    printf("BPM CheckedReadPage tried to Evict frame but frame %d is pinned.\n", frame_opt.value()->frame_id_);
    return std::nullopt;
  }
  printf("BPM CheckedReadPage, evicting frame id %d holding page id %d.\n", frame_opt.value()->frame_id_,
         frame_opt.value()->page_id_);
  // Now that we checked the pin count, we can evict/remove from replacer
  replacer_->Remove(evicted_frame_id.value());
  std::shared_ptr<FrameHeader> frame = frame_opt.value();
  BUSTUB_ENSURE(frame->page_id_ != INVALID_PAGE_ID, "invalid page id from frame header ptr");
  if (!frame->page_id_) {
    // Error, the evicted FrameHeader should have a page associated with it
    lock.unlock();
    return std::nullopt;
  }

  // Flush the old evicted frame from memory to disk if dirty
  if (frame->is_dirty_) {
    // Schedule the I/O to write the page data from memory to disk
    ScheduleIO(true, frame->GetDataMut(), frame->page_id_);
    frame->is_dirty_ = false;
  }

  // Handle reseting frame and reassigning new page info
  BUSTUB_ENSURE(page_table_.count(frame->page_id_) > 0,
                "BufferPoolManager::CheckedReadPage Evicted page not found in page table");
  page_table_.erase(frame->page_id_);
  frame->Reset();
  // Load page into frame
  frame->UpdatePageId(page_id);
  page_table_[page_id] = frame->frame_id_;
  // Call dibs on the frame
  frame->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame->frame_id_);
  replacer_->SetEvictable(frame->frame_id_, false);

  // Schedule the I/O to read the page data from disk to memory
  ScheduleIO(false, frame->GetDataMut(), page_id);

  // Release BPM's latch before acquiring the frame's latch
  lock.unlock();
  // Create the read guard
  ReadPageGuard guard(page_id, frame, replacer_, bpm_latch_);
  return guard;

  // lock.unlock();
  // return std::nullopt; */

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
  // LOG_FUNCTION_CALL();
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  return std::move(guard_opt).value();
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
  // LOG_FUNCTION_CALL();
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  // // return std::move(guard_opt.value());
  // BUSTUB_ENSURE(guard_opt.has_value(), "CheckedReadPage failed");
  // ReadPageGuard guard = std::move(*guard_opt);  // move from a named object, not a temp
  // std::cerr << "Returning guard from ReadPage with frame = " << guard.frame_.get() << "\n";
  return std::move(guard_opt).value();
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
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // LOG_FUNCTION_CALL();
  printf("BPM Flush page id %d.\n", page_id);
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    // printf("BPM Flush, page %d is not in the page table\n", page_id);
    return false;
  }
  auto frame_header_ptr = frames_[page_table_[page_id]];
  frame_header_ptr->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame_header_ptr->frame_id_);
  replacer_->SetEvictable(frame_header_ptr->frame_id_, false);
  if (frame_header_ptr->is_dirty_) {
    ScheduleIO(/*is_write=*/true, frame_header_ptr, page_id);
    frame_header_ptr->is_dirty_ = false;
  }
  frame_header_ptr->pin_count_.fetch_sub(1);
  frame_header_ptr->needs_to_be_reloaded_ = true;
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(abeach): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // LOG_FUNCTION_CALL();
  printf("BPM Flush all pages\n");
  for (const auto &pair : page_table_) {
    FlushPage(pair.first);
  }
}

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
  // LOG_FUNCTION_CALL();
  std::scoped_lock<std::mutex> guard(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  // Page exists in the page table
  // auto frame_opt = FindFrameHeader(it->second);  // it -> second get's the value, frame_id
  // BUSTUB_ENSURE(frame_opt.has_value(),
  // "BufferPoolManager::GetPinCount FrameHeader should exist after retrieving frame page table");
  frame_id_t frame_id = it->second;
  printf("BPM GetPinCount for page id %d is %lu.\n", page_id, frames_[frame_id]->pin_count_.load());
  return frames_[frame_id]->pin_count_.load();
}

/**
 * @brief Helper Function - Find Frame Header
 * Retrieves the frame header from the Buffer Pool Manager's list of frame via the frame id
 *
 * @param frame_id the frame ID that we want to find
 * @return std::optional<std::shared_ptr<FrameHeader>>
 */
auto BufferPoolManager::FindFrameHeader(frame_id_t frame_id) -> std::optional<std::shared_ptr<FrameHeader>> {
  // LOG_FUNCTION_CALL();
  // printf("Before Find Frame Header: trying to find frame id  %d, all of the frames in frames are below:\n",
  // frame_id); for (std::shared_ptr<FrameHeader> frame_header_ptr : frames_) {
  //   printf("frame id %d, ", frame_header_ptr->frame_id_);
  // }
  // printf("\n");
  // printf("BPM::FindFrameHeader, searching for frame %d \n", frame_id);
  return frames_[frame_id];
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
void BufferPoolManager::ScheduleIO(bool is_write, std::shared_ptr<FrameHeader> &frame_header_ptr,
                                   page_id_t page_id)  // NOLINT(readability-non-const-parameter)
{
  // LOG_FUNCTION_CALL();
  printf("BPM ScheduleIO %s operation on page id %d.\n", is_write ? "write" : "read", page_id);
  std::promise<bool> p;
  auto f = p.get_future();
  DiskRequest req{
      .is_write_ = is_write, .data_ = frame_header_ptr->GetDataMut(), .page_id_ = page_id, .callback_ = std::move(p)};
  disk_scheduler_->Schedule(std::move(req));
  if (f.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    throw std::runtime_error("BufferPoolManager::ScheduleIO timeout on page_id " + std::to_string(page_id));
  }
  f.get();
}

/**
 * @brief Load the requested page into an existing frame.
 *
 * LOCKED(bpm_latch): Must be called while the bpm_latch_ is held.
 *
 * Once we've got the `bpm_latch_` and a free frame, this function does all the
 * required bookkeeping to load that page into memory by putting it into the
 * frame's data.
 *
 * @param page_id The page to load from disk.
 * @param frame_id An index into our `frames_` vector.
 * @return Largely for succinctness at the call site, returns `frames_[frame_id]`.
 */
auto BufferPoolManager::LoadPageIntoFrame(page_id_t page_id, frame_id_t frame_id) -> std::shared_ptr<FrameHeader> {
  printf("BPM LoadPageIntoFrame page id %d into frame id %d.\n", page_id, frame_id);

  page_table_[page_id] = frame_id;
  auto frame_header_ptr = frames_[frame_id];
  frame_header_ptr->UpdatePageId(page_id);
  ScheduleIO(false, frame_header_ptr, page_id);
  frame_header_ptr->needs_to_be_reloaded_ = false;
  return frame_header_ptr;
}

/**
 * @brief Grab or create a frame holding page_id if possible.
 *
 * No lock necessary, it calls dibs by adjusting frame header's pin count, recording access,
 * and setting the frame as not evictable. In doing so, it LOCKS the replacer.
 */
void BufferPoolManager::CallDibsOnHeader(const std::shared_ptr<FrameHeader> &frame_header_ptr, bool is_write) {
  frame_header_ptr->pin_count_.fetch_add(1);
  // Debug
  size_t curr_pin_count = frame_header_ptr->pin_count_.load();
  if (curr_pin_count == 2) {
    printf(
        "DEBUG: CallDibsOnHeader from %s read guard, increased pin count of page id %d from %lu which is more than 1\n",
        is_write ? "write" : "read", frame_header_ptr->page_id_, curr_pin_count);
  }
  replacer_->RecordAccess(frame_header_ptr->frame_id_);
  replacer_->SetEvictable(frame_header_ptr->frame_id_, false);
}

/**
 * @brief Grab or create a frame holding `page_id` if possible.
 *
 * LOCKS(bpm_latch_), DIBS(frame_header_ptr->pin_count_), UNLOCK(bpm_latch_)
 * if no frames are available, no dibs is called.
 */
auto BufferPoolManager::GetOrMakeFrameForGuard(page_id_t page_id, bool is_write)
    -> std::optional<std::shared_ptr<FrameHeader>> {
  std::unique_lock<std::mutex> lock(*bpm_latch_);  // automatically unlocks when out of scope
  std::shared_ptr<FrameHeader> frame_header_ptr;
  if (page_table_.find(page_id) != page_table_.end()) {
    // no additional i/o is needed since page is in page table, frame is in frames
    frame_header_ptr = frames_[page_table_[page_id]];
  } else if (!free_frames_.empty()) {
    // lots of available memory
    auto frame_to_fill_id = free_frames_.front();
    free_frames_.pop_front();
    frame_header_ptr = LoadPageIntoFrame(page_id, frame_to_fill_id);
  } else {
    // do LRUK things
    auto evicted_frame_id_opt = TryToEvictPage(is_write);
    if (!evicted_frame_id_opt.has_value()) {  // no evictable frames (or the proposed victim was pinned)
      return std::nullopt;                    // no need to manually unlock the bpm_latch
    }
    frame_header_ptr = LoadPageIntoFrame(page_id, evicted_frame_id_opt.value());
  }
  // Debug
  size_t curr_pin_count = frame_header_ptr->pin_count_.load();
  if (curr_pin_count == 1) {
    printf(
        "DEBUG: GetOrMakeFrameForGuard from %s page guard, about to increased pin count of page id %d from %lu to 2 "
        "which is not allowed\n",
        is_write ? "write" : "read", page_id, curr_pin_count);
  }
  CallDibsOnHeader(frame_header_ptr, is_write);
  if (frame_header_ptr->needs_to_be_reloaded_) {
    frame_header_ptr = LoadPageIntoFrame(page_id, page_table_[page_id]);
  }
  // safe to wait to create the page guard until later because we've called dibs on the page.
  // no need to manually unlock the bpm_latch, just wait for end of scope.
  return frame_header_ptr;
}

/**
 * @brief Evict one frame if possible.
 *
 * LOCKED(bpm_latch): Must be called while the bpm_latch_ is held.
 *
 * Does nothing if no frames are evictable or if the LRU K-cache's proposed
 * "victim" frame is pinned.
 *
 * Otherwise, evicts the frame chosen by our `replacer_` and returns the
 * corresponding frame_id.
 *
 * @param page_id The page to load from disk, if possible.
 * @return The id of the frame that was evicted. While the bpm_latch remains
 * held,this is safe to pass to `LoadPageIntoFrame`.
 */
auto BufferPoolManager::TryToEvictPage(bool is_write) -> std::optional<frame_id_t> {
  auto victim_frame_id_opt = replacer_->FindEvictFrameWithoutEvict();
  if (!victim_frame_id_opt.has_value()) {  // nothing evictable
    printf("BPM TryToEvictPage but nothing is evictable.\n");

    return std::nullopt;
  }
  auto victim_frame_id = victim_frame_id_opt.value();
  auto victim_frame_header_ptr = frames_[victim_frame_id];
  if (victim_frame_header_ptr->pin_count_.load() > 0) {  // can't evict a pinned frame
    printf("BPM TryToEvictPage but victim is pinned.\n");

    return std::nullopt;
  }
  // now that we checked the pin count, we can effectively evict this frame
  printf("BPM TryToEvictPage, evicting frame id %d holding page id %d.\n", victim_frame_header_ptr->frame_id_,
         victim_frame_header_ptr->page_id_);

  replacer_->Remove(victim_frame_id);
  auto victim_page_id = (victim_frame_header_ptr)->page_id_;
  // BUSTUB_ENSURE(victim_page_id != -1, "invalid page id from frame header ptr")
  // Debug
  size_t curr_pin_count = victim_frame_header_ptr->pin_count_.load();
  if (curr_pin_count == 1) {
    printf(
        "DEBUG: TryToEvictPage from %s page guard, about increased pin count of page id %d from %lu to 2 which is not "
        "allowed\n",
        is_write ? "write" : "read", victim_page_id, curr_pin_count);
  }
  CallDibsOnHeader(victim_frame_header_ptr, is_write);
  if ((victim_frame_header_ptr)->is_dirty_) {  // "flush" the data
    ScheduleIO(true, victim_frame_header_ptr, victim_page_id);
    victim_frame_header_ptr->is_dirty_ = false;
  }
  victim_frame_header_ptr->pin_count_.fetch_sub(1);
  // reset frame
  page_table_.erase(victim_page_id);
  victim_frame_header_ptr->Reset();
  return victim_frame_id;
}

}  // namespace bustub
