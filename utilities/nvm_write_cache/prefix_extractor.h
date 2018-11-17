//
// Created by 张艺文 on 2018/11/2.
//

#pragma once

#include <cstdint>
#include <string>
#include <include/rocksdb/slice.h>

namespace rocksdb{

    class PrefixExtractor{
    public:
        PrefixExtractor() =default;

        virtual ~PrefixExtractor() =default;

        virtual std::string operator ()(const char* input, size_t length) = 0;

    };


    class SimplePrefixExtractor: public PrefixExtractor{
    public:
        explicit SimplePrefixExtractor(uint16_t prefix_bits_);

        ~SimplePrefixExtractor() =default;

        std::string operator()(const char* input, size_t length);

        static SimplePrefixExtractor* NewSimplePrefixExtractor(uint16_t prefix_bits);


    private:
        uint16_t prefix_bits_;
    };
}// end rocksdb

