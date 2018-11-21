//
// Created by zzyyy on 2018/11/2.
// 胡泽鑫负责
//

#pragma once

#include <queue>
#include "utilities/nvm_write_cache/nvm_write_cache.h"
#include "utilities/nvm_write_cache/nvm_cache_options.h"


namespace rocksdb{

    class FixedRange;

    struct FixedRangeChunkBasedCacheStats{

        uint64_t  used_bits_;

        std::unordered_map<std::string, uint64_t> range_list_;

        std::vector<std::string*> chunk_bloom_data_;

    };

    struct CompactionItem{
        FixedRange* pending_compated_range_;
        InternalKey start_key_, end_key_;
        uint64_t range_size_, chunk_num_;
    };

    class FixedRangeChunkBasedNVMWriteCache: public NVMWriteCache{

    public:

        explicit FixedRangeChunkBasedNVMWriteCache(FixedRangeBasedOptions* cache_options_);

        ~FixedRangeChunkBasedNVMWriteCache() override;

        // insert data to cache
        // insert_mark is (uint64_t)range_id
        Status Insert(const Slice& cached_data, void* insert_mark) override;

        // get data from cache
        Status Get(const Slice& key, std::string* value) override;

        // get iterator of the total cache
        Iterator* NewIterator() override;

        // return there is need for compaction or not
        bool NeedCompaction() override {return !range_queue_.empty();}

        //get iterator of data that will be drained
        GetCompactionData(CompactionItem* compaction_item);
        //CompactionItem* GetCompactionData();

        // add a range with a new prefix to range mem
        // return the id of the range
        uint64_t NewRange(const std::string& prefix);

        // get internal options of this cache
        const FixedRangeBasedOptions* internal_options(){return internal_options_;}

        // get stats of this cache
        FixedRangeChunkBasedCacheStats* stats(){return cache_stats_;}

    private:
        const FixedRangeBasedOptions* internal_options_;
        FixedRangeChunkBasedCacheStats* cache_stats_;
        std::queue<CompactionItem*> range_queue_;
        uint64_t range_seq_;


    };
}