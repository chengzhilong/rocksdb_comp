//
// Created by 张艺文 on 2018/11/5.
//
#include "util/coding.h"
#include "include/rocksdb/filter_policy.h"

#include "chunk.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"


namespace rocksdb{
    ArrayBasedChunk::ArrayBasedChunk() {
        raw_data_.clear();
        entry_offset_.clear();
    }

    void ArrayBasedChunk::Insert(const Slice &key, const Slice &value) {
        unsigned int total_size = key.size_ + value.size_ + 8 + 8;
        raw_data_.resize(raw_data_.size() + total_size);

        raw_data_.append(key.data_, key.size_);
        PutFixed64(&raw_data_, key.size_);
        raw_data_.append(value.data_, value.size_);
        PutFixed64(&raw_data_, value.size_);

        entry_offset_.push_back(now_offset_);
        now_offset_ += total_size;
    }

    std::string *ArrayBasedChunk::Finish() {
        raw_data_.resize(raw_data_.size() + 8 * entry_offset_.size());
        for (auto offset : entry_offset_) {
            PutFixed64(&raw_data_, offset);
        }

        auto *result = new std::string(raw_data_);

        return result;
    }

    BuildingChunk::BuildingChunk(const FilterPolicy* filter_policy) : chunk_(new ArrayBasedChunk()),
                                                                      filter_policy_(filter_policy) {
        if(filter_policy_ == nullptr){
            printf("empty filter policy\n");
        }

    }

    BuildingChunk::~BuildingChunk() {
        delete chunk_;
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


    std::string* BuildingChunk::Finish() {
        std::string *chunk_data, *chunk_bloom_data;
        chunk_data = chunk_->Finish();
        chunk_bloom_data = new std::string();

        filter_policy_->CreateFilter(&keys_[0], keys_.size(), chunk_bloom_data);

        chunk_data->resize(chunk_data->size() + chunk_bloom_data->size());
        chunk_data->append(*chunk_bloom_data);
        delete chunk_bloom_data;
        return chunk_data;
    }
}
