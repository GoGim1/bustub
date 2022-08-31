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

LRUReplacer::LRUReplacer(size_t num_pages) : lru_limit_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> lock(m_);
    if (lru_.empty())
        return false;
    else {
        *frame_id = lru_.front();
        lru_.pop_front();
        return true;
    } 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(m_);
    lru_.remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(m_);
    for (auto id: lru_) {
        if (frame_id == id) 
            return;
    }
    if (lru_.size() == lru_limit_) 
        lru_.pop_front();
    lru_.push_back(frame_id);  
}

size_t LRUReplacer::Size() { 
    std::lock_guard<std::mutex> lock(m_);
    return lru_.size();    
}

}  // namespace bustub
