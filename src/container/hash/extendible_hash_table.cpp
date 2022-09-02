//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager->NewPage(&directory_page_id_)->GetData());
  directory_page->IncrGlobalDepth();
  directory_page->SetPageId(directory_page_id_);

  page_id_t bucket_page_id_0;
  buffer_pool_manager_->NewPage(&bucket_page_id_0);
  directory_page->SetBucketPageId(0, bucket_page_id_0);
  directory_page->SetLocalDepth(0, 1);

  page_id_t bucket_page_id_1;
  buffer_pool_manager_->NewPage(&bucket_page_id_1);
  directory_page->SetBucketPageId(1, bucket_page_id_1);
  directory_page->SetLocalDepth(1, 1);

  buffer_pool_manager_->UnpinPage(bucket_page_id_0, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id_1, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto directory_page = FetchDirectoryPage();
  auto page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(page_id);
  bool ret = bucket_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  if (bucket_page->IsFull()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return SplitInsert(transaction, key, value);
  }
  else {
    auto ret = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, ret);
    return ret;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto directory_page = FetchDirectoryPage();
  auto bucket_id = KeyToDirectoryIndex(key, directory_page);
  auto bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  bool incr_global_depth = false;
  if (directory_page->GetLocalDepth(bucket_id) == directory_page->GetGlobalDepth()) {
    directory_page->IncrGlobalDepth();
    incr_global_depth = true;
  } 
  auto split_bucket_id = 1 << directory_page->GetLocalDepth(bucket_id) | bucket_id;
  directory_page->IncrLocalDepth(bucket_id);
  auto new_local_depth = directory_page->GetLocalDepth(bucket_id);
  page_id_t split_bucket_page_id;
  auto split_bucket_page =
    reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_bucket_page_id)->GetData());
  directory_page->SetBucketPageId(split_bucket_id, split_bucket_page_id);
  directory_page->SetLocalDepth(split_bucket_id, new_local_depth);

  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (bucket_page->IsReadable(i)) {
       auto key = bucket_page->KeyAt(i);
       auto value = bucket_page->ValueAt(i);
       auto insert_id = Hash(key) & ((1 << new_local_depth) - 1);
       if (insert_id == split_bucket_id) {
        split_bucket_page->Insert(key, value, comparator_);
        bucket_page->RemoveAt(i);
       }
    }
  }

  if (incr_global_depth) {
    auto global_depth = directory_page->GetGlobalDepth();
    for (size_t i = 1 << (global_depth - 1); i < directory_page->Size(); i++) {
      if (i == split_bucket_id) continue;

      auto redirect_bucket_idx = i & ((1 << (global_depth - 1)) -1);
      directory_page->SetBucketPageId(i, directory_page->GetBucketPageId(redirect_bucket_idx));
      directory_page->SetLocalDepth(i, directory_page->GetLocalDepth(redirect_bucket_idx));
    }
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, incr_global_depth);
  buffer_pool_manager_->UnpinPage(bucket_id, true);
  buffer_pool_manager_->UnpinPage(split_bucket_id, true);
  return Insert(transaction, key, value);
  
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  auto ret = bucket_page->Remove(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, ret);
  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
