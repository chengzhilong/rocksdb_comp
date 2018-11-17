#ifndef FIXED_RANGE_CHUNK_BASED_NVM_WRITE_CACHE_H
#define FIXED_RANGE_CHUNK_BASED_NVM_WRITE_CACHE_H


#include <queue>
#include <rocksdb/iterator.h>
#include "utilities/nvm_write_cache/nvm_write_cache.h"
#include "utilities/nvm_write_cache/nvm_cache_options.h"

#include <libpmemobj++/pool.hpp>

#include <fixed_range_tab.h>
#include <pmem_hash_map.h>

namespace rocksdb {

using std::string;
using namespace pmem;
using namespace pmem::obj;

struct FixedRangeChunkBasedCacheStats{
  uint64_t  used_bits_;
  std::unordered_map<std::string, uint64_t> range_list_;
  std::vector<std::string*> chunk_bloom_data_;
};

struct CompactionItem {
  FixedRangeTab* pending_compated_range_;
  Slice start_key_, end_key_;
  uint64_t range_size_, range_rum_;
};

class FixedRangeChunkBasedNVMWriteCache : public NVMWriteCache
{
public:
  FixedRangeChunkBasedNVMWriteCache(const string& file, const string& layout);
  ~FixedRangeChunkBasedNVMWriteCache();

  // insert data to cache
  // insert_mark is (uint64_t)range_id
//  Status Insert(const Slice& cached_data, void* insert_mark) override;

  // get data from cache
  Status Get(const Slice& key, std::string* value) override;


  // get iterator of the total cache
  Iterator* NewIterator() override;

  // 获取range_mem_id对应的RangeMemtable结构
//  FixedRangeTab* GetRangeMemtable(uint64_t range_mem_id);

  // return there is need for compaction or not
  bool NeedCompaction() override {return !range_queue_.empty();}

  //get iterator of data that will be drained
  // get 之后释放没有 ?
  CompactionItem& GetCompactionData() {
    CompactionItem *item = range_queue_.front();
//    range_queue_.pop();
    return item;
  }
  void addCompactionRangeTab(FixedRangeTab *tab);

  // add a range with a new prefix to range mem
  // return the id of the range
  uint64_t NewRange(const std::string& prefix);

  // get internal options of this cache
  const FixedRangeBasedOptions* internal_options(){return internal_options_;}

  // get stats of this cache
  FixedRangeChunkBasedCacheStats* stats(){return cache_stats_;}
private:
  string file_path;
  const string LAYOUT;
  const size_t POOLSIZE;

  //  persistent_queue<FixedRangeTab> range_mem_list_;
//  persistent_map<range, FixedRangeTab> range2tab;
  //  persistent_queue<uint64_t> compact_queue_;

  pool<p_range::pmem_hash_map> pop;

private:
  const FixedRangeBasedOptions* internal_options_;
  FixedRangeChunkBasedCacheStats* cache_stats_;
  std::queue<CompactionItem> range_queue_;
  uint64_t range_seq_;
};

} // namespace rocksdb
#endif // FIXED_RANGE_CHUNK_BASED_NVM_WRITE_CACHE_H
