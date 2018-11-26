#include "persistent_chunk_iterator.h"

namespace rocksdb {

PersistentChunkIterator::PersistentChunkIterator(p_buf data,
                                                 size_t size, Arena *arena)
  :data_(data), arena_(arena)
{
  // keysize | key | valsize | val | ... | 1st pair offset
  // | 2nd pair offset | ... | num of pairs
  size_t nPairs;
  p_buf nPairsOffset = data + size - sizeof(nPairs);
  nPairs = DecodeFixed64(nPairsOffset.get());

//  nPairs = *(reinterpret_cast<size_t*>(nPairsOffset));
  vKey_.reserve(nPairs);
  vValue_.reserve(nPairs);

  p_buf metaOffset = nPairsOffset - sizeof(metaOffset) * nPairs;// 0 first

  for (int i = 0; i < nPairs; ++i) {
    size_t pairOffset = DecodeFixed64(metaOffset.get());

    memcpy(&pairOffset, metaOffset, sizeof(pairOffset));
//    *(reinterpret_cast<size_t*>(metaOffset));
    p_buf pairAddr = data + pairOffset;

    // key size
    size_t _size = DecodeFixed64(pairAddr.get());
    // key
    vKey_.emplace_back(_size, pairAddr + sizeof(_size));
//    size_t _size = *(reinterpret_cast<size_t*>(pairAddr));
//    vKey_.emplace_back(pairAddr + sizeof(_size), _size);


    pairAddr += sizeof(_size) + _size;
    // value size
    _size = DecodeFixed64(pairAddr.get());
    // value
    vValue_.emplace_back(_size, pairAddr + sizeof(_size));
//    _size = *(reinterpret_cast<size_t*>(pairAddr));
//    vValue_.emplace_back(pairAddr + sizeof(_size), _size);

    // next pair
    metaOffset += sizeof(pairOffset);
  }
}

} // namespace rocksdb

