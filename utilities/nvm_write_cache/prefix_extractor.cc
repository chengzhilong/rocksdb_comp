//
// Created by 张艺文 on 2018/11/2.
//

#include "prefix_extractor.h"

namespace rocksdb{

    SimplePrefixExtractor::SimplePrefixExtractor(uint16_t prefix_bits):PrefixExtractor(), prefix_bits_(prefix_bits){}

    std::string SimplePrefixExtractor::operator()(const char *input, size_t length) {
        return length > prefix_bits_ ? std::string(input, prefix_bits_) : std::string(input, length);
    }


    SimplePrefixExtractor* SimplePrefixExtractor::NewSimplePrefixExtractor(uint16_t prefix_bits) {
        return new SimplePrefixExtractor(prefix_bits);
    }


}//end rocksdb