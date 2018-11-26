#ifndef CHUNKBLK_H
#define CHUNKBLK_H

#include <stdint.h>
#include <stddef.h>

namespace rocksdb {

/* *
 * --------------
 * | bloom data | // bloom_bits
 * --------------
 * | chunk  len | // sizeof(size_t)
 * --------------
 * | chunk data | // chunkLen
 * --------------
 * */
class ChunkBlk {
public:
    explicit ChunkBlk(size_t bloom_bits, size_t offset, size_t chunkLen)
            : bloom_bits_(bloom_bits), offset_(offset), chunkLen_(chunkLen) {

    }

    size_t getDatOffset() {
        return offset_ + bloom_bits_ + sizeof(chunkLen_);
    }

    size_t bloom_bits_;
    size_t offset_; // offset of bloom filter in range buffer
    size_t chunkLen_;

    // kv data start at offset + CHUNK_BLOOM_FILTER_SIZE + sizeof(chunLen)
};

} // namespace rocksdb

#endif // CHUNKBLK_H