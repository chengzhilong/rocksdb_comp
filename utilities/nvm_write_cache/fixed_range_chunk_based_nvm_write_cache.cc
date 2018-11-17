//
// Created by zzyyy on 2018/11/2.
//

#include "utilities/nvm_write_cache/fixed_range_chunk_based_nvm_write_cache.h"
namespace rocksdb {
    FixedRangeChunkBasedNVMWriteCache::FixedRangeChunkBasedNVMWriteCache(
            rocksdb::FixedRangeBasedOptions *cache_options_)
            :
                internal_options_(cache_options_),
                cache_stats_(new FixedRangeChunkBasedCacheStats),
                range_seq_(0)
                {
        cache_stats_->range_list_.clear();
        cache_stats_->chunk_bloom_data_.clear();
        cache_stats_->used_bits_ = 0;

    }

    FixedRangeChunkBasedNVMWriteCache::~FixedRangeChunkBasedNVMWriteCache() {
        delete cache_stats_;
    }

    Status FixedRangeChunkBasedNVMWriteCache::Get(const rocksdb::Slice &key, std::string *value) {
        return Status::OK();
    }

    Status FixedRangeChunkBasedNVMWriteCache::Insert(const rocksdb::Slice &cached_data, void *insert_mark) {
        printf("insert data\n");
        cache_stats_->used_bits_ += cached_data.size_;
        return Status::OK();
    }

    uint64_t FixedRangeChunkBasedNVMWriteCache::NewRange(const std::string &prefix) {
        printf("new range %s\n", prefix.c_str());
        return ++range_seq_;
    }

    Iterator* FixedRangeChunkBasedNVMWriteCache::NewIterator() {
        return nullptr;
    }

    CompactionItem* FixedRangeChunkBasedNVMWriteCache::GetCompactionData() {
        return nullptr;
    }






}// end rocnsdb
