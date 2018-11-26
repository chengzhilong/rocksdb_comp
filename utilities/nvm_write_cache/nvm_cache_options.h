//
// Created by 张艺文 on 2018/11/2.
//

#pragma once

#include "include/rocksdb/options.h"
#include "utilities/nvm_write_cache/prefix_extractor.h"
#include "utilities/nvm_write_cache/nvm_write_cache.h"
namespace rocksdb{

    class NVMWriteCache;
    //class PrefixExtractor;
    class FixedRangeChunkBasedNVMWriteCache;

    struct PMemInfo{
        std::string pmem_path_;
        uint64_t pmem_size_;
    };

    enum NVMCacheType{
        kRangeFixedChunk,
        kRangeDynamic,
        kTreeBased,
    };

    enum DrainStrategy{
        kCompaction,
        kNoDrain,
    };

struct FixedRangeBasedOptions{

    const uint16_t chunk_bloom_bits_ = 16;
    const uint16_t prefix_bits_ = 3;
    PrefixExtractor* prefix_extractor_ = nullptr;
    const FilterPolicy* filter_policy_ = nullptr;
    //const uint64_t range_num_threshold_ = 0;
    const size_t range_size_ = 1 << 27;

    FixedRangeBasedOptions(
            uint16_t chunk_bloom_bits,
            uint16_t prefix_bits,
            PrefixExtractor* prefix_extractor,
            const FilterPolicy* filter_policy,
            //uint64_t range_num_threashold,
            uint64_t range_size)
            :
            chunk_bloom_bits_(chunk_bloom_bits),
            prefix_bits_(prefix_bits),
            prefix_extractor_(prefix_extractor),
            filter_policy_(filter_policy),
            //range_num_threshold_(range_num_threashold),
            range_size_(range_size){

    }

};

struct DynamicRangeBasedOptions{

};

struct TreeBasedOptions{

};


    struct NVMCacheOptions{
        NVMCacheOptions();
        explicit NVMCacheOptions(Options& options);
        ~NVMCacheOptions();

        bool use_nvm_write_cache_;
        bool reset_nvm_write_cache;
        PMemInfo pmem_info_;
        NVMCacheType nvm_cache_type_;
        NVMWriteCache* nvm_write_cache_;
        DrainStrategy drain_strategy_;

        static FixedRangeChunkBasedNVMWriteCache* NewFixedRangeChunkBasedCache(NVMCacheOptions* nvm_cache_options,
                FixedRangeBasedOptions* foptions);

    };



} //end rocksdb

