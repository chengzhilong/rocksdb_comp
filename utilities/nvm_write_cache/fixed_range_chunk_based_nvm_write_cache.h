#pragma once

#include <queue>
#include <unordered_map>
#include "rocksdb/iterator.h"
#include "utilities/nvm_write_cache/nvm_write_cache.h"
#include "utilities/nvm_write_cache/nvm_cache_options.h"

#include "libpmemobj++/p.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/make_persistent.hpp"
#include "libpmemobj++/make_persistent_array.hpp"

#include "fixed_range_tab.h"
#include "pmem_hash_map.h"

using std::string;
using std::unordered_map;
using namespace pmem::obj;
using p_range::pmem_hash_map;
//using p_range::p_node_t;

namespace rocksdb {

class InternalIterator;

static inline int
file_exists(char const *file) {
    return access(file, F_OK);
}

struct CompactionItem {
    FixedRangeTab *pending_compated_range_;

    explicit CompactionItem(FixedRangeTab *range)
            : pending_compated_range_(range) {}

    CompactionItem(const CompactionItem &item)
            : pending_compated_range_(item.pending_compated_range_) {}
};

struct ChunkMeta {
    string prefix;
    Slice cur_start;
    Slice cur_end;
};

// TODO:是否需要自定义Allocator
/*class PersistentAllocator {
public:
    explicit PersistentAllocator(persistent_ptr<char[]> raw_space, uint64_t total_size) {
        raw_ = raw_space;
        total_size_ = total_size;
        cur_ = 0;
    }

    ~PersistentAllocator() = default;

    char *Allocate(size_t alloca_size) {
        char *alloc = &raw_[0] + cur_;
        cur_ = cur_ + alloca_size;
        return alloc;
    }

    uint64_t Remain() {
        return total_size_ - cur_;
    }

    uint64_t Capacity() {
        return total_size_;
    }


private:
    persistent_ptr<char[]> raw_;
    p<uint64_t> total_size_;
    p<uint64_t> cur_;

};*/

using p_buf = persistent_ptr<char[]>;

class FixedRangeChunkBasedNVMWriteCache : public NVMWriteCache {
public:
    explicit FixedRangeChunkBasedNVMWriteCache(
            const FixedRangeBasedOptions *ioptions,
            const string &file, uint64_t pmem_size,
            bool reset = false);

    ~FixedRangeChunkBasedNVMWriteCache() override;

    // insert data to cache
    // insert_mark is (uint64_t)range_id
//  Status Insert(const Slice& cached_data, void* insert_mark) override;

    // get data from cache
    Status Get(const InternalKeyComparator &internal_comparator, const Slice &key, std::string *value) override;

    void AppendToRange(const InternalKeyComparator &icmp, const char *bloom_data, const Slice &chunk_data,
                       const ChunkMeta &meta);

    // get iterator of the total cache
    InternalIterator *NewIterator(const InternalKeyComparator *icmp, Arena *arena) override;

    // return there is need for compaction or not
    bool NeedCompaction() override { return !vinfo_->range_queue_.empty(); }

    //get iterator of data that will be drained
    // get 之后释放没有 ?
    void GetCompactionData(CompactionItem *compaction) {
        *compaction = vinfo_->range_queue_.front();
        vinfo_->range_queue_.pop();
    }

    // get internal options of this cache
    const FixedRangeBasedOptions *internal_options() { return vinfo_->internal_options_; }

    void MaybeNeedCompaction();

    void RangeExistsOrCreat(const std::string &prefix);

private:

    persistent_ptr<NvRangeTab> NewContent(const string& prefix, size_t bufSize);
    FixedRangeTab *NewRange(const std::string &prefix);

    void RebuildFromPersistentNode();

    struct PersistentInfo {
        p<bool> inited_;
        p<uint64_t> allocated_bits_;
        persistent_ptr<pmem_hash_map<NvRangeTab> > range_map_;
        // TODO: allocator分配的空间没法收回
        //persistent_ptr<PersistentAllocator> allocator_;
    };

    pool<PersistentInfo> pop_;
    persistent_ptr<PersistentInfo> pinfo_;

    struct VolatileInfo {
        const FixedRangeBasedOptions *internal_options_;
        unordered_map<string, FixedRangeTab> prefix2range;
        std::queue<CompactionItem> range_queue_;

        explicit VolatileInfo(const FixedRangeBasedOptions *ioptions)
                : internal_options_(ioptions) {}
    };

    VolatileInfo *vinfo_;
};

} // namespace rocksdb