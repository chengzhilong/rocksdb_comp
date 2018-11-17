#include "fixed_range_tab.h"

#include <table/merging_iterator.h>

#include <persistent_chunk.h>

namespace rocksdb {

using pmem::obj::persistent_ptr;
#define MAX_BUF_LEN 4096

//POBJ_LAYOUT_BEGIN(range_mem);
//POBJ_LAYOUT_ROOT(range_mem, struct my_root);
//POBJ_LAYOUT_TOID(range_mem, struct foo_el);
//POBJ_LAYOUT_TOID(range_mem, struct bar_el);
//POBJ_LAYOUT_END(range_mem);

struct my_root {
  size_t length; // mark end of chunk block sequence
  unsigned char data[MAX_BUF_LEN];
};



FixedRangeTab::FixedRangeTab(size_t chunk_count, char *data, int filterLen)
  : chunk_sum_size(0),
    MAX_CHUNK_SUM_SIZE(64 * 1024 * 1024)
{

}

FixedRangeTab::~FixedRangeTab()
{

}

//| used_bits ... | 预设 start | 预设 end |


//| chunk blmFilter | chunk ...   |  不定长
//| chunk blmFilter | chunk ...  |  不定长
//| chunk blmFilter | chunk ...    |  不定长
//| real_start | real_end |


InternalIterator* FixedRangeTab::NewInternalIterator(
    ColumnFamilyData *cfd, Arena *arena)
{
  InternalIterator* internal_iter;
  MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(),
                                          arena);
  // TODO
  // 预设 range 持久化
//  char *chunkBlkOffset = data_ + sizeof(stat.used_bits_) + sizeof(stat.start_)
//      + sizeof(stat.end_);

  persistent_ptr<char[]> chunkBlkOffset = node_in_pmem_map->buf;

  PersistentChunk pchk;
  for (int i = 0; i < info.chunk_num; ++i) {
//    chunk_blk *blk = reinterpret_cast<chunk_blk*>(chunkBlkOffset);
    persistent_ptr<char[]> sizeOffset = chunkBlkOffset + CHUNK_BLOOM_FILTER_SIZE;
    size_t blkSize;
    memcpy(&blkSize, sizeOffset, sizeof(blkSize));
    pchk.reset(CHUNK_BLOOM_FILTER_SIZE, blkSize, sizeOffset + sizeof(blkSize));
    merge_iter_builder.AddIterator(pchk.NewIterator(arena));

    chunkBlkOffset = sizeOffset + sizeof(blkSize) + blkSize;
  }

  internal_iter = merge_iter_builder.Finish();
}

void FixedRangeTab::Append(const char *bloom_data,
                           const Slice &chunk_data,
                           const Slice &new_start,
                           const Slice &new_end)
{
  if (chunk_sum_size + chunk_data.size_ >= MAX_CHUNK_SUM_SIZE
      || info.chunk_num > ) {
    // TODO
    // flush
    chunk_sum_size = 0;
  }

  // TODO
  // 事务更换成 cpp ?
  // 开始追加
  TX_BEGIN(pop) {
    /* TX_STAGE_WORK */
    rootp = POBJ_ROOT(pop, my_root);
    size_t cur_len = node_in_pmem_map->dataLen;
    size_t chunk_blk_len = CHUNK_BLOOM_FILTER_SIZE + sizeof(chunk_data.size_)
        + chunk_data.size_;
    // 添加持久化范围
//    TX_ADD_FIELD(rootp, length);
    unsigned char *dest = node_in_pmem_map->buf + cur_len;
    pmemobj_tx_add_range_direct(dest, chunk_blk_len);
    // 复制 chunk block
    memcpy(dest, bloom_data, CHUNK_BLOOM_FILTER_SIZE);
    memcpy(dest+CHUNK_BLOOM_FILTER_SIZE, chunk_data.size_, sizeof(chunk_data.size_));
    dest += CHUNK_BLOOM_FILTER_SIZE+sizeof(chunk_data.size_);
    memcpy(dest, chunk_data.data_, chunk_data.size_);
    // 更新总长度
    node_in_pmem_map->dataLen = cur_len + chunk_blk_len;
    chunk_sum_size += chunk_data.size_;
    // blk 偏移
//    psttChunkList.push_back(cur_len);

    info.chunk_num++;
    //        dest += chunk_data.size_;
    //        info.update(new_start, new_end);
    //        memcpy(dest, info, sizeof)
    //    } TX_ONCOMMIT {
    //        /* TX_STAGE_ONCOMMIT */
    //    } TX_ONABORT {
    //        /* TX_STAGE_ONABORT */
    //    } TX_FINALLY {
    //        /* TX_STAGE_FINALLY */
  } TX_END
}

void FixedRange::Release()
{

}

} // namespace rocksdb
