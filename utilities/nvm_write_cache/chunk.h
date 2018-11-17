//
// Created by 张艺文 on 2018/11/5.
//

#pragma once


#include <cstdint>
#include <string>
#include <vector>


namespace rocksdb{
    class Slice;
    class FilterPolicy;

    class ArrayBasedChunk{
    public:
        explicit ArrayBasedChunk();

        ~ArrayBasedChunk() =default;

        void Insert(const Slice& key, const Slice& value);

        std::string* Finish();

        // Iterator* NewIterator();

        // void ParseRawData();

    private:
        std::string raw_data_;
        std::vector<uint64_t> entry_offset_;
        uint64_t num_entries;
        uint64_t now_offset_;

    };

    class BuildingChunk{
    public:
        explicit BuildingChunk(const FilterPolicy* filter_policy);
        ~BuildingChunk();

        void Insert(const Slice& key, const Slice& value);

        std::string* Finish();

        uint64_t NumEntries();

    private:
        uint64_t num_entries_;
        ArrayBasedChunk* chunk_;
        std::vector<Slice> keys_;
        const FilterPolicy* filter_policy_;
    };
}
