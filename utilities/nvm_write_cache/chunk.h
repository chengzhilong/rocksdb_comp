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
        explicit BuildingChunk(const FilterPolicy* filter_policy, const std::string& prefix);
        ~BuildingChunk();

        void Insert(const Slice& key, const Slice& value);

        std::string* Finish(char** bloom_data, Slice& cur_start, Slice& cur_end);

        uint64_t NumEntries();

    private:
        std::string prefix_;
        uint64_t num_entries_;
        ArrayBasedChunk* chunk_;
        std::vector<Slice> keys_;
        const FilterPolicy* filter_policy_;
    };
}
