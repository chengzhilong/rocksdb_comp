#ifndef PERSISTENT_CHUNK_ITERATOR_H
#define PERSISTENT_CHUNK_ITERATOR_H

#include <vector>

#include <rocksdb/iterator.h>
#include <table/merging_iterator.h>

#include <libpmemobj++/persistent_ptr.hpp>

namespace rocksdb {

class PersistentChunkIterator : public InternalIterator
{
  using std::vector;
  using std::pair;
  using namespace pmem::obj;

  using p_buf = persistent_ptr<char[]>;
public:
  explicit PersistentChunkIterator(p_buf data, size_t size, Arena* arena);

  Slice key() override {
    Slice slc(vKey_.at(current_).second, vKey_.at(current_).first);
    return slc;
//    return vKey_.at(current_);

    // TODO
    // 读取一致性问题 是否要事务 防止flush时只读取到部分数据
//    char *pairOffset = data_ + pair_offset.at(current_);
//    size_t keySize = *(const_cast<size_t*>(pairOffset));

//    // TODO
//    char *dest;
//    memcpy(dest, pairOffset + sizeof(keySize), keySize);
  }
  Slice value() override {
    Slice slc(vValue_.at(current_).second, vValue_.at(current_).first);
    return slc;
//    return vValue_.at(current_);

    // TODO
    // 同 Key()
//    char *pairOffset = data_ + pair_offset.at(current_);
//    size_t keySize = *(const_cast<size_t*>(pairOffset));

//    char *valsizeOffset = pairOffset + sizeof(keySize) + keySize;
//    size_t valSize = *(const_cast<size_t*>(valsizeOffset));

//    // TODO
//    char *dest;
//    memcpy(dest, valsizeOffset + sizeof(valSize), valSize);
  }

  void SeekToFirst() override { current_ = 0; }
  void SeekToLast() override { current_ = vKey_.size() - 1; }
  bool Valid() override { return current_ < vKey_.size(); }
  void Next() override {
    assert(Valid());
    ++current_;
  }
  void Prev() override {
    assert(Valid());
    --current_;
  }

//  char *data_; // 数据起点
  p_buf data_;

  Arena* arena_;
//  vector<char*> pair_offset;
//  vector<Slice> vKey_;
//  vector<Slice> vValue_;


  vector<pair<size_t, p_buf>> vKey_;
  vector<pair<size_t, p_buf>> vValue_;
  size_t current_;
  //  size_t nPairs;
};

} // namespace rocksdb

#endif // PERSISTENT_CHUNK_ITERATOR_H
