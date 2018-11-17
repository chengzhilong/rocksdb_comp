//
// Created by 张艺文 on 2018/11/2.
//

#pragma once

#include <include/rocksdb/iterator.h>
#include <unordered_map>
#include <vector>


namespace rocksdb{


    class NVMWriteCache{
    public:
        NVMWriteCache() =default;

        virtual ~NVMWriteCache() = default;

        virtual Status Insert(const Slice& cached_data, void* insert_mark = nullptr) = 0;

        virtual Status Get(const Slice& key, std::string* value) = 0;

        virtual Iterator* NewIterator() = 0;

        //virtual Iterator* GetDraineddata() = 0;

        virtual bool NeedCompaction() = 0;

        //virtual void* GetOptions() = 0;

        //virtual CacheStats* GetStats() = 0;

    };

} // end rocksdb
