//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}
  

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// 刷新目标页面到磁盘
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lk{latch_};
  if(page_id == INVALID_PAGE_ID){
    return false;
  }
  auto iter = page_table_.find(page_id);
  // 如果page不在页表中
  if(iter == page_table_.end()){
    return false;
  }
  // 根据frame_id 来获得page对象
  frame_id_t frame = iter->second;
  Page *page = &pages_[frame];
  disk_manager_->WritePage(page_id,page->GetData());
  page->is_dirty_ = false; // 写入磁盘之后重新设置脏页面
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lk{latch_};
  auto iter = page_table_.begin();
  while(iter != page_table_.end()){
    page_id_t page_id = iter->first;
    frame_id_t frame = iter->second;
    Page *page = &pages_[frame];
    disk_manager_->WritePage(page_id, page->GetData());
    iter ++;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lk{latch_};
  frame_id_t frame = -1;
  Page *page = nullptr;
  // 如果freelist中就去replacer中找，没有返回nullptr
  if(!free_list_.empty()){
    frame = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame];
  } else if(replacer_->Victim(&frame)) {
    page = &pages_[frame];
    if(page->IsDirty()){
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    return nullptr;
  }
  page_id_t new_page_id = AllocatePage();
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();

  // 将申请到的page添加到page_table中，pin该页面并返回数据
  page_table_[new_page_id] = frame;
  *page_id = new_page_id;
  replacer_->Pin(frame);
  return page; 
  
}
//从缓冲池中获取请求的页面
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.」
  std::scoped_lock lk{latch_};
  auto iter = page_table_.find(page_id);
  if(iter != page_table_.end()){
    frame_id_t frame = iter->second;
    Page *page = &pages_[frame];
    page->pin_count_ += 1;
    replacer_->Pin(frame);
    return page;
  }
  frame_id_t frame = -1;
  Page *page = nullptr;
  if(!free_list_.empty()){
    frame = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame];
  } else if(replacer_->Victim(&frame)){
    page = &pages_[frame];
    if(page->IsDirty()){
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    return nullptr;
  }
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  // 
  disk_manager_->ReadPage(page_id, page->GetData());
  //new出后添加到pg_table,pin该page
  page_table_[page_id] = frame;
  replacer_->Pin(frame);
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lk{latch_};
  auto iter = page_table_.find(page_id);
  if(iter == page_table_.end()){
    return true;
  }
  frame_id_t frame = iter->second;
  Page *page = &pages_[frame];
  if(page->GetPinCount() > 0 ){ // 该页面已经被其他页面占用
    return false;
  }
  if(page->IsDirty()){
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  replacer_->Pin(frame);
  page_table_.erase(page->page_id_);
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  free_list_.push_back(frame);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { return false; }

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
