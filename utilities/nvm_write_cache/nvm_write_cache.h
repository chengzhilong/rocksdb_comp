//
// Created by 张艺文 on 2018/11/2.
//

#pragma once

#include <include/rocksdb/iterator.h>
#include <unordered_map>
#include <vector>


namespace rocksdb {

class InternalKeyComparator;
class Arena;
class InternalIterator;

class NVMWriteCache {
public:
    NVMWriteCache() = default;

    virtual ~NVMWriteCache() = default;

    virtual Status Insert(const Slice &cached_data, void *insert_mark = nullptr) = 0;

    virtual Status Get(const InternalKeyComparator &internal_comparator, const Slice &key, std::string *value) = 0;

    virtual InternalIterator *NewIterator(const InternalKeyComparator *icmp, Arena *arena) = 0;

    virtual bool NeedCompaction() = 0;

};

} // end rocksdb
