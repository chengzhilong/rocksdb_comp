#ifndef FIXED_RANGE_ITERATOR_H
#define FIXED_RANGE_ITERATOR_H

#include <cstdint>

#include "table/internal_iterator.h"

namespace rocksdb {

// 参考 testutil.h

class FixRangeIterator : public InternalIterator
{
public:
  explicit FixRangeIterator(size_t bloomFilterLen) {

  }

  void Next() override {++current_;}
  void Prev() override {--current_;}
  virtual bool Valid() const override { return current_ < keys_.size(); }

  Slice Key() override {

  }
  Slice Value() override {

  }


  size_t current_;
};

} // namespace rocksdb

#endif // FIXED_RANGE_ITERATOR_H
