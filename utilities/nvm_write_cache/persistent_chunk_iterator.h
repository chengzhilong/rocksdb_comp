#ifndef PERSISTENT_CHUNK_ITERATOR_H
#define PERSISTENT_CHUNK_ITERATOR_H

#include <vector>

#include "rocksdb/iterator.h"
#include "table/merging_iterator.h"

#include "libpmemobj++/persistent_ptr.hpp"

namespace rocksdb {

class PersistentChunkIterator : public InternalIterator {
    using std::vector;
    using std::pair;
    using namespace pmem::obj;

    using p_buf = persistent_ptr<char[]>;
public:
    explicit PersistentChunkIterator(p_buf data, size_t size, Arena *arena);

    Slice key() override {
        _Dat_ &dat = vKey_.at(current_);
        return Slice(dat.buf.get(), dat.size);


        // TODO
        // 读取一致性问题 是否要事务 防止flush时只读取到部分数据

    }

    Slice value() override {
        _Dat_ &dat = vValue_.at(current_);
        return Slice(dat.buf.get(), dat.size);
//    Slice slc(dat.buf, dat.size);
//    return slc;
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

    // add by xiaohu
    void SeekTo(size_t idx) { current_ = idx; }

    size_t count() { return vKey_.size(); }

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

    Arena *arena_;
//  vector<char*> pair_offset;
//  vector<Slice> vKey_;
//  vector<Slice> vValue_;

    struct _Dat_ {
        size_t size;
        p_buf buf;

        _Dat_(size_t _size, p_buf _buf) : size(_size), buf(_buf) {}
    };

    vector<_Dat_> vKey_;
    vector<_Dat_> vValue_;

//  vector<pair<size_t, p_buf>> vKey_;
    size_t current_;
    //  size_t nPairs;
};

} // namespace rocksdb

#endif // PERSISTENT_CHUNK_ITERATOR_H