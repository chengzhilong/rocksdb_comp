//
// Created by 张艺文 on 2018/11/5.
//
#include "util/coding.h"
#include "include/rocksdb/filter_policy.h"

#include "chunk.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"


namespace rocksdb {
ArrayBasedChunk::ArrayBasedChunk() {
    raw_data_.clear();
    entry_offset_.clear();
}


/* ------------
 * | key_size | // 64bit
 * ------------
 * |    key   |
 * ------------
 * |value_size| // 64bit
 * ------------
 * |   value  |
 * ------------
 * */
void ArrayBasedChunk::Insert(const Slice &key, const Slice &value) {
    unsigned int total_size = key.size_ + value.size_ + 8 + 8;
    raw_data_.resize(raw_data_.size() + total_size);

    PutFixed64(&raw_data_, key.size_);
    raw_data_.append(key.data_, key.size_);
    PutFixed64(&raw_data_, value.size_);
    raw_data_.append(value.data_, value.size_);


    entry_offset_.push_back(now_offset_);
    now_offset_ += total_size;
}

/*
 * |      chunk1     |
 * |      chunk2     |
 * |       ...       |
 * | chunk offset 1  |
 * | chunk offset 2  |
 * |       ...       |
 * |    chunk num    |
 * */
std::string *ArrayBasedChunk::Finish() {
    raw_data_.resize(raw_data_.size() + sizeof(uint64_t) * (entry_offset_.size() + 1));
    for (auto offset : entry_offset_) {
        PutFixed64(&raw_data_, offset);
    }

    PutFixed64(&raw_data_, entry_offset_.size());

    auto *result = new std::string(raw_data_);

    return result;
}

BuildingChunk::BuildingChunk(const FilterPolicy *filter_policy, const std::string &prefix)
        : prefix_(prefix),
          chunk_(new ArrayBasedChunk()),
          filter_policy_(filter_policy) {
    if (filter_policy_ == nullptr) {
        printf("empty filter policy\n");
    }

}

BuildingChunk::~BuildingChunk() {
    delete chunk_;
    for (auto tmp : keys_) {
        delete[] tmp.data_;
    }
    keys_.clear();
}

uint64_t BuildingChunk::NumEntries() {
    return num_entries_;
}

void BuildingChunk::Insert(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    chunk_->Insert(key, value);
    char *key_rep = new char[key.size_];
    memcpy(key_rep, key.data_, key.size_);
    keys_.emplace_back(key_rep);
}


std::string *BuildingChunk::Finish(char **bloom_data, rocksdb::Slice &cur_start, rocksdb::Slice &cur_end) {
    std::string *chunk_data, *chunk_bloom_data;
    // get kv data
    chunk_data = chunk_->Finish();

    // get bloom data
    chunk_bloom_data = new std::string();
    filter_policy_->CreateFilter(&keys_[0], keys_.size(), chunk_bloom_data);
    char *raw_bloom_data = new char[chunk_bloom_data->size()];
    memcpy(raw_bloom_data, chunk_bloom_data->c_str(), chunk_bloom_data->size());
    *bloom_data = raw_bloom_data;

    // get key range
    cur_start = keys_[0];
    cur_end = keys_[keys_.size() - 1];

    delete chunk_bloom_data;
    return chunk_data;
}
}
