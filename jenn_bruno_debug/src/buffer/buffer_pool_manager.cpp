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
#include <memory>
#include <mutex>
#include <optional>
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

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
  page_id_ = -1;
}

void FrameHeader::UpdatePageId(page_id_t new_page_id) { page_id_ = new_page_id; }

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
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
  // BRUNO: For now, this seems to assume that the BufferPoolManager is only
  // created once per database file, and that something else is in charge of
  // making sure we don't re-use page_id's between restarts of the overall
  // program.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));  // frames_ is vector so use frame_id as idx to get FrameHeader
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
 * You can assume that you will never run out of disk space (via `DiskScheduler::IncreaseDiskSpace`),
 * so this function _cannot_ fail.
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
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  printf("BPM DeletePage page id %d.\n", page_id);

  std::scoped_lock<std::mutex> lock(*bpm_latch_);  // automatically unlocks when out of scope
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;  // page doesn't exist (previously deleted)
  }
  auto frame_header_ptr = frames_[iter->second];
  if (frame_header_ptr->pin_count_.load() > 0) {  // we can't delete the page if somebody is accessing it
    return false;
  }
  auto frame_id = page_table_[page_id];
  page_table_.erase(page_id);  // no need to flush first since we're nuking the page
  free_frames_.push_back(frame_id);
  replacer_->Remove(frame_id);
  frame_header_ptr->Reset();
  disk_scheduler_->DeallocatePage(page_id);
  return true;  // page successfully deleted
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
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  printf("BPM CheckedWritePage page id %d.\n", page_id);

  auto frame_header_ptr_opt = GetOrMakeFrameForGuard(page_id);
  if (!frame_header_ptr_opt.has_value()) {  // no frame available to load page into, so no new pins were added
    return std::nullopt;
  }
  return WritePageGuard(page_id, frame_header_ptr_opt.value(), replacer_, bpm_latch_);
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
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  printf("BPM CheckedReadPage page id %d.\n", page_id);

  auto frame_header_ptr_opt = GetOrMakeFrameForGuard(page_id);
  if (!frame_header_ptr_opt.has_value()) {  // no frame available to load page into, so no new pins were added
    return std::nullopt;
  }
  return ReadPageGuard(page_id, frame_header_ptr_opt.value(), replacer_, bpm_latch_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
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
  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk. Does NOT need any page guard.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  printf("BPM Flush page id %d.\n", page_id);

  std::unique_lock<std::mutex> lock(*bpm_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_header_ptr = frames_[page_table_[page_id]];
  CallDibsOnHeader(frame_header_ptr);
  if (frame_header_ptr->is_dirty_) {
    ScheduleIO(/*is_write=*/true, frame_header_ptr, page_id);
    frame_header_ptr->is_dirty_ = false;
  }
  frame_header_ptr->pin_count_.fetch_sub(1);  // unpin the header we called dibs on
  frame_header_ptr->needs_to_be_reloaded_ = true;

  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 */
void BufferPoolManager::FlushAllPages() {
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
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock<std::mutex> lock(*bpm_latch_);  // automatically unlocks when out of scope
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return std::nullopt;
  }
  frame_id_t frame_id = iter->second;
  printf("BPM GetPinCount for page id %d is %lu.\n", page_id, frames_[frame_id]->pin_count_.load());

  return frames_[frame_id]->pin_count_.load();  // atomic load for multi-threaded environment
}

/**
 * @brief Schedules the DiskRequest to either read data from disk to memory or write data (if frame is dirty) from
 * memory to disk.
 */
void BufferPoolManager::ScheduleIO(bool is_write, std::shared_ptr<FrameHeader> &frame_header_ptr, page_id_t page_id) {
  printf("BPM ScheduleIO %s operation on page id %d.\n", is_write ? "write" : "read", page_id);

  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  DiskRequest req{.is_write_ = is_write,
                  .data_ = frame_header_ptr->GetDataMut(),
                  .page_id_ = page_id,
                  .callback_ = std::move(promise)};
  disk_scheduler_->Schedule(std::move(req));
  if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    throw std::runtime_error("BufferPoolManager::ScheduleIO timeout on page_id " + std::to_string(page_id));
  }
  future.get();
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
auto BufferPoolManager::TryToEvictPage() -> std::optional<frame_id_t> {
  auto victim_frame_id_opt = replacer_->FindVictimFrame();
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
  BUSTUB_ENSURE(victim_page_id != -1, "invalid page id from frame header ptr")
  CallDibsOnHeader(victim_frame_header_ptr);
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

/**
 * @brief Grab or create a frame holding page_id if possible.
 *
 * No lock necessary, it calls dibs by adjusting frame header's pin count, recording access,
 * and setting the frame as not evictable. In doing so, it LOCKS the replacer.
 */
void BufferPoolManager::CallDibsOnHeader(const std::shared_ptr<FrameHeader> &frame_header_ptr) {
  frame_header_ptr->pin_count_.fetch_add(1);
  replacer_->RecordAccess(frame_header_ptr->frame_id_);
  replacer_->SetEvictable(frame_header_ptr->frame_id_, false);
}

/**
 * @brief Grab or create a frame holding `page_id` if possible.
 *
 * LOCKS(bpm_latch_), DIBS(frame_header_ptr->pin_count_), UNLOCK(bpm_latch_)
 * if no frames are available, no dibs is called.
 */
auto BufferPoolManager::GetOrMakeFrameForGuard(page_id_t page_id) -> std::optional<std::shared_ptr<FrameHeader>> {
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
    auto evicted_frame_id_opt = TryToEvictPage();
    if (!evicted_frame_id_opt.has_value()) {  // no evictable frames (or the proposed victim was pinned)
      return std::nullopt;                    // no need to manually unlock the bpm_latch
    }
    frame_header_ptr = LoadPageIntoFrame(page_id, evicted_frame_id_opt.value());
  }
  CallDibsOnHeader(frame_header_ptr);
  if (frame_header_ptr->needs_to_be_reloaded_) {
    frame_header_ptr = LoadPageIntoFrame(page_id, page_table_[page_id]);
  }
  // safe to wait to create the page guard until later because we've called dibs on the page.
  // no need to manually unlock the bpm_latch, just wait for end of scope.
  return frame_header_ptr;
}

}  // namespace bustub
