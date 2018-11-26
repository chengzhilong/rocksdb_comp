//
// Created by 张艺文 on 2018/11/2.
//

#include "nvm_cache_options.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"


namespace rocksdb{
    NVMCacheOptions::NVMCacheOptions()
        :   use_nvm_write_cache_(false)
    {

    }

    NVMCacheOptions::NVMCacheOptions(Options &options)
        :   use_nvm_write_cache_(options.nvm_cache_options->use_nvm_write_cache_),
            reset_nvm_write_cache(options.nvm_cache_options->reset_nvm_write_cache),
            pmem_info_(options.nvm_cache_options->pmem_info_),
            nvm_cache_type_(options.nvm_cache_options->nvm_cache_type_),
            nvm_write_cache_(options.nvm_cache_options->nvm_write_cache_),
            drain_strategy_(options.nvm_cache_options->drain_strategy_)
    {

    }


    NVMCacheOptions::~NVMCacheOptions() {
        delete nvm_write_cache_;
    }

    FixedRangeChunkBasedNVMWriteCache* NVMCacheOptions::NewFixedRangeChunkBasedCache(NVMCacheOptions* nvm_cache_options,
            FixedRangeBasedOptions *foptions) {
        return new FixedRangeChunkBasedNVMWriteCache(foptions,
                                                     nvm_cache_options->pmem_info_.pmem_path_,
                                                     nvm_cache_options->pmem_info_.pmem_size_,
                                                     nvm_cache_options->reset_nvm_write_cache);
    }


}//end rocksdb
