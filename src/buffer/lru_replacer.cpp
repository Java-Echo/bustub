//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    size = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    //c++17中关于scoped_lock的使用
    std::scoped_lock lk{mu};
    if(lru_cache.empty()){
        return false;
    }
    *frame_id = lru_cache.back();
    lru_hash.erase(*frame_id);
    lru_cache.pop_back();
    return true;
}
// 被pin中的page不应成为victim，从replacer中移除
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock lk{mu};
    auto iter = lru_hash.find(frame_id);
    if(iter == lru_hash.end()){
        // 没找到
        return;
    }
    lru_cache.erase(iter->second);
    lru_hash.erase(iter);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock lk{mu};
    if(lru_hash.find(frame_id) != lru_hash.end()){
        return;
    }
    if(lru_cache.size() >= size){
        return ;
    }
    //根据lru，添加到链表头
    lru_cache.push_front(frame_id);
    lru_hash[frame_id] = lru_cache.begin();
}

size_t LRUReplacer::Size() { 
    std::scoped_lock lk{mu};
    return lru_cache.size();
}

}  // namespace bustub
