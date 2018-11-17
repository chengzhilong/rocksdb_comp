//
// Created by 张艺文 on 2018/11/14.
//

#pragma once
#include <string>
#include <cassert>
#include <cstring>
#include "util/random.h"
#include "utilities/nvm_write_cache/skiplist/libpmemobj++/p.hpp"
#include "utilities/nvm_write_cache/skiplist/libpmemobj++/persistent_ptr.hpp"
#include "utilities/nvm_write_cache/skiplist/libpmemobj++/make_persistent.hpp"
#include "utilities/nvm_write_cache/skiplist/libpmemobj++/make_persistent_array.hpp"
#include "utilities/nvm_write_cache/skiplist/libpmemobj++/transaction.hpp"
#include "utilities/nvm_write_cache/skiplist/libpmemobj++/pool.hpp"

using namespace pmem::obj;

namespace rocksdb{
    struct Node {
        explicit Node(const std::string &key, int height)
                :
                key_(key){
            //next_by_insert_ = nullptr;
        };

        ~Node() = default;

        Node* Next(int n) {
            assert(n >= 0);
            return next_[n];
        }

        void SetNext(int n, Node* next) {
            assert(n >= 0);
            next_[n] = next;
        };

        //p<int> height_;
        std::string key_;
        int index_;
        int height;
        //Node* next_by_insert_;
        Node* next_[1];
    };

    class VolatileSkipList {
    public:
        explicit VolatileSkipList(int32_t max_height = 12, int32_t branching_factor = 4);

        ~VolatileSkipList();

        void Insert(const char *key, int height, int seq);

        void Print() const;

        void GetIndex(uint64_t& size, std::vector<int>& result);

        int head(){return head_->Next(0)->index_;}

        //bool Contains(const char *key);

    private:
        Node* head_;
        Node** prev_;
        uint32_t prev_height_;
        uint16_t kMaxHeight_;
        uint16_t kBranching_;
        uint32_t kScaledInverseBranching_;

        uint16_t max_height_;

        //Node* first_inserted_;
        //Node* prev_inserted_;
        //int seq_num_;

        inline int GetMaxHeight() const {
            return max_height_;
        }

        Node* NewNode(const std::string &key, int height, int seq);


        bool Equal(const char *a, const char *b) {
            return strcmp(a, b) == 0;
        }

        bool LessThan(const char *a, const char *b) {
            return strcmp(a, b) < 0;
        }

        bool KeyIsAfterNode(const std::string& key, Node* n) const;

        Node* FindGreaterOrEqual(const std::string& key) const;

        Node* FindLessThan(const std::string& key, Node *prev[]) const;

    };


    class PersistentBatchUpdateSkiplist{
    public:
        explicit PersistentBatchUpdateSkiplist(pool_base& pop, int32_t max_height = 12, int32_t branching_factor = 4);
        ~PersistentBatchUpdateSkiplist();

        void Insert(const std::string& key);

    private:

        int RandomHeight();

        uint64_t AppendToKeyLog(const char* data, size_t size);

        void GetKey(uint64_t offset, std::string& key, uint64_t& accumu_height, uint64_t& prev_accumu_height);

        void TransferIntoVolatile();

        void BuildNewListAndPersist();

        pool_base& pop_;
        p<int> seq_;

        persistent_ptr<int[]> off_array_;
        p<size_t> array_cur_;
        p<size_t> head_;
        p<uint64_t> pendding_sort_;



        persistent_ptr<char[]> key_log_;
        p<size_t> key_cur_;


        persistent_ptr<int[]> index_log_;
        p<size_t > index_size_;
        p<size_t > index_cur_;


        const uint16_t kMaxHeight_;
        const uint16_t kBranching_;
        const uint32_t kScaledInverseBranching_;

        p<uint64_t> count_;
        VolatileSkipList* vlist_;
    };

    struct SkiplistWrapper{
        p<bool> inited = false;
        persistent_ptr<PersistentBatchUpdateSkiplist> batchskiplist = nullptr;

        void Init(pool_base& pop, uint32_t max_height, uint32_t branching_factor);
    };
} //end rocksdb


