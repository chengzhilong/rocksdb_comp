#ifndef PERSISTENT_RANGE_MEM_H
#define PERSISTENT_RANGE_MEM_H

#include <list>
#include <db/db_impl.h>

//#include "libpmemobj.h"
#include "libpmemobj++/p.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/make_persistent.hpp"
#include "libpmemobj++/make_persistent_array.hpp"

#include "persistent_chunk.h"
#include "pmem_hash_map.h"
#include "nvm_cache_options.h"
#include "chunkblk.h"

using namespace pmem::obj;
using namespace p_range;
using std::string;

namespace rocksdb {

using pmem::obj::persistent_ptr;

using p_buf = persistent_ptr<char[]>;
struct NvRangeTab {
public:
    NvRangeTab(pool_base &pop, const string &prefix, uint64_t range_size);

    uint64_t hashCode(const string& prefix) {
        return CityHash64WithSeed(prefix, prefix.size(), 16);
    }

    char* GetRawBuf(){return buf.get();}

    // 通过比价前缀，比较两个NvRangeTab是否相等
    bool equals(const string &prefix);

    bool equals(p_buf &prefix, size_t len);

    bool equals(NvRangeTab &b);

    p<uint64_t> hash_;
    p<size_t> prefixLen; // string prefix_ tail 0 not included
    p_buf prefix_; // prefix
    p_buf key_range_; //key range
    p<size_t> chunk_num_;
    p<uint64_t> seq_num_;

    p<size_t> bufSize; // capacity
    p<size_t> dataLen; // exact data len
    persistent_ptr<NvRangeTab> extra_buf;
    p_buf buf; // buf data


};


struct Usage {
    uint64_t chunk_num;
    uint64_t range_size;
    InternalKey start, end;
};


class FixedRangeTab {


public:
    //FixedRangeTab(pool_base &pop, FixedRangeBasedOptions *options);

    FixedRangeTab(pool_base &pop, FixedRangeBasedOptions *options,
                  persistent_ptr<NvRangeTab> &nonVolatileTab);

    //FixedRangeTab(pool_base &pop, p_node pmap_node_, FixedRangeBasedOptions *options);

//  FixedRangeTab(pool_base& pop, p_node pmap_node_, FixedRangeBasedOptions *options);

    ~FixedRangeTab() = default;

public:
    //void reservePersistent();

    // 返回当前RangeMemtable中所有chunk的有序序列
    // 基于MergeIterator
    // 参考 DBImpl::NewInternalIterator
    InternalIterator *NewInternalIterator(const InternalKeyComparator *icmp, Arena *arena);

    Status Get(const InternalKeyComparator &internal_comparator, const Slice &key,
               std::string *value);

    persistent_ptr<NvRangeTab> getPersistentData() { return nonVolatileTab_; }

    // 返回当前RangeMemtable是否正在被compact
    bool IsCompactWorking() { return in_compaction_; }

    // 设置compaction状态
    void SetCompactionWorking(bool working) {
        pendding_clean_ = blklist.size();
        in_compaction_ = working;
    }

    // 将新的chunk数据添加到RangeMemtable
    void Append(const InternalKeyComparator &icmp,
                const char *bloom_data, const Slice &chunk_data,
                const Slice &start, const Slice &end);

    void SetExtraBuf(persistent_ptr<NvRangeTab> extra_buf);

    Usage RangeUsage();

    // 释放当前RangeMemtable的所有chunk以及占用的空间
    void Release();

    // 重置Stat数据以及bloom filter
    void CleanUp();

    uint64_t max_range_size() {
        return nonVolatileTab_->bufSize;
    }

#ifdef TAB_DEBUG
void GetProperties();
#endif

private:

    void RebuildBlkList();

    uint64_t max_chunk_num_to_flush() const {
        // TODO: set a max chunk num
        return 1024;
    }

    // 返回当前RangeMem的真实key range（stat里面记录）
    void GetRealRange(Slice &real_start, Slice &real_end);

    Status searchInChunk(PersistentChunkIterator *iter,
                         const InternalKeyComparator &icmp,
                         const Slice &key, std::string *value);

    Slice GetKVData(char *raw, uint64_t item_off);

    void CheckAndUpdateKeyRange(const InternalKeyComparator &icmp, const Slice &new_start, const Slice &new_end);

    void ConsistencyCheck();



    // persistent info
    //p_node pmap_node_;
    pool_base &pop_;
    persistent_ptr<NvRangeTab> nonVolatileTab_;


    // volatile info
    const FixedRangeBasedOptions *interal_options_;
    vector<ChunkBlk> blklist;
    char *raw_;
    bool in_compaction_;
    size_t pendding_clean_;


};

} // namespace rocksdb

#endif // PERSISTENT_RANGE_MEM_H
