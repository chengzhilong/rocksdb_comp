//
// Created by 张艺文 on 2018/11/5.
//

#pragma once
#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

#include <cstring>
#include <string>
#include <io.h>
#include <ctime>
#include "util/random.h"
#include "utilities/nvm_write_cache/libpmemobj++/p.hpp"
#include "utilities/nvm_write_cache/libpmemobj++/persistent_ptr.hpp"
#include "utilities/nvm_write_cache/libpmemobj++/transaction.hpp"
#include "utilities/nvm_write_cache/libpmemobj++/pool.hpp"
#include "utilities/nvm_write_cache/libpmemobj++/make_persistent.hpp"
#include "utilities/nvm_write_cache/libpmemobj++/make_persistent_array.hpp"

using namespace pmem::obj;

namespace rocksdb {

    static inline int
    file_exists(char const *file)
    {
        return access(file, F_OK);
    }

    const int kMaxHeight = 12;

    struct p_string{
        persistent_ptr<char[]> data_;
        p<size_t> size_;

        p_string(pool_base& pop, const std::string& src){
            transaction::run(pop, [&]{
                data_ = make_persistent<char[]>(src.size() + 1);
                memcpy(&data_[0], src.c_str(), src.size());
                data_[src.size()] = 0;
                size_ = src.size();
                printf("p_string = %s\n", &data_[0]);
            });

        }

        int compare(const std::string& right){
            return strcmp(&data_[0], right.c_str());
        }

        const char* data(){
            return &data_[0];
        }

         size_t size(){
            return size_;
        }
    };

    struct Node {
        explicit Node() {
        };

        ~Node() = default;

        persistent_ptr<Node> Next(int n) {
            assert(n >= 0);
            return next_[n];
        }

        void SetNext(int n, persistent_ptr<Node> next) {
            assert(n >= 0);
            printf("next_[%d] @ [%p]\n", n, next_[n]);
            next_[n] = next;
        };

        persistent_ptr<p_string> key_;
        persistent_ptr<persistent_ptr<Node>[]> next_;
    };

    class persistent_SkipList {
    public:
        explicit persistent_SkipList(pool_base &pop, int32_t max_height = 12, int32_t branching_factor = 4);

        ~persistent_SkipList(){}

        void Insert(pool_base& pop, const char *key);

        void Print() const;

        //bool Contains(const char *key);

    private:
        persistent_ptr<Node> head_;
        persistent_ptr<persistent_ptr<Node>[]> prev_;
        p<uint32_t> prev_height_;
        p<uint16_t> kMaxHeight_;
        p<uint16_t> kBranching_;
        p<uint32_t> kScaledInverseBranching_;

        p<uint16_t> max_height_;


        inline int GetMaxHeight() const {
            return max_height_;
        }

        persistent_ptr<Node> NewNode(pool_base& pop, const std::string &key, int height);

        int RandomHeight();

        bool Equal(const char *a, const char *b) {
            return strcmp(a, b) == 0;
        }

        bool LessThan(const char *a, const char *b) {
            return strcmp(a, b) < 0;
        }

        bool KeyIsAfterNode(const std::string& key, persistent_ptr<Node> n) const;

        persistent_ptr<Node> FindGreaterOrEqual(const std::string& key) const;

        persistent_ptr<Node> FindLessThan(const std::string& key, persistent_ptr<persistent_ptr<Node>[]> prev = nullptr) const;

        //persistent_ptr<Node> FindLast() const;


    };


    persistent_SkipList::persistent_SkipList(pool_base &pop, int32_t max_height, int32_t branching_factor)
            :
            kMaxHeight_(static_cast<uint16_t>(max_height)),
            kBranching_(static_cast<uint16_t>(branching_factor)),
            kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),

            max_height_(1) {
        head_ = NewNode(pop, " ", max_height);

        transaction::run(pop, [&]{
            prev_ = make_persistent<persistent_ptr<Node>[]>(max_height);
            /*for(int i = 0; i < max_height; i++){
                printf("new p<node >%p\n", prev_[i]);
            }*/
            for (int i = 0; i < kMaxHeight_; i++) {
                head_->SetNext(i, nullptr);
                prev_[i] = head_;
                //printf("prev[%d] @ [%p]is %s\n", i, prev_[i], prev_[i]== nullptr?"null":"not null");
                //printf("prev[%d] is %s\n", 0, prev_[0]== nullptr?"null":"not null");
            }
            prev_height_ = 1;
            //printf("end transaction prev[%d] is %s\n", 0, prev_[0]== nullptr?"null":"not null");
        });

        //printf("prev[%d] is %s\n", 0, prev_[0]== nullptr?"null":"not null");
        //printf("prev height is %d\n", prev_height_);
    }

    persistent_ptr<Node> persistent_SkipList::NewNode(pool_base& pop, const std::string &key, int height) {
        persistent_ptr<Node> n;
        transaction::run(pop, [&] {
            n = make_persistent<Node>();
            n->key_ = make_persistent<p_string>(pop, key);
            n->next_ = make_persistent<persistent_ptr<Node>[]>(height);
            for(int i = 0; i < height; i++){
                printf("new p<node >%p\n", n->next_[i]);
            }
        });
        return n;
    }

    int persistent_SkipList::RandomHeight() {
        auto rnd = Random::GetTLSInstance();
        int height = 1;
        while (height < kMaxHeight_ && rnd->Next() < kScaledInverseBranching_) {
            height++;
        }
        return height;
    }

    // when n < key returns true
    // n should be at behind of key means key is after node
    bool persistent_SkipList::KeyIsAfterNode(const std::string& key, persistent_ptr<Node> n) const {
        printf("n is %s\n", n == nullptr?"null":"not null");
        return (n != nullptr) && (n->key_->compare(key));
    }

    persistent_ptr<Node> persistent_SkipList::FindLessThan(const std::string &key,
                                                           persistent_ptr<persistent_ptr<Node>[]> prev) const {
        persistent_ptr<Node> x = head_;
        int level = GetMaxHeight() - 1;
        persistent_ptr<Node> last_not_after;
        while(true){
            persistent_ptr<Node> next = x->Next(level);
            if(next != last_not_after && KeyIsAfterNode(key, next)){
                x = next;
            }else{
                prev[level] = x;
                if(level ==0 ){
                    return x;
                }else{
                    last_not_after = next;
                    level--;
                }
            }
        }
    }

    void persistent_SkipList::Insert(pool_base& pop, const char *key) {
        // key < prev[0]->next(0) && prev[0] is head or key < prev[0]
        printf("prev[0] is %s\n", prev_[0]== nullptr?"null":"not null");
        if (!KeyIsAfterNode(key, prev_[0]->Next(0)) &&
            (prev_[0] == head_ || KeyIsAfterNode(key, prev_[0]))) {
            for (uint32_t i = 1; i < prev_height_; i++) {
                prev_[i] = prev_[0];
            }
        } else {
            FindLessThan(key, prev_);
        }

        int height = RandomHeight();
        if(height > GetMaxHeight()){
            for(int i = GetMaxHeight(); i < height; i++){
                prev_[i] = head_;
            }
            max_height_ = static_cast<uint16_t >(height);
        }

        transaction::run(pop, [&]{
            persistent_ptr<Node> x = NewNode(pop, key, height);
            for(int i = 0; i < height; i++){
                x->SetNext(i, prev_[i]->Next(i));
                prev_[i]->SetNext(i ,x);
            }
            prev_[0] = x;
            prev_height_ = static_cast<uint16_t >(height);
        });

    }


    persistent_ptr<Node> persistent_SkipList::FindGreaterOrEqual(const std::string &key) const {
        persistent_ptr<Node> x = head_;
        int level = GetMaxHeight() - 1;
        persistent_ptr<Node> last_bigger;
        while(true){
            persistent_ptr<Node> next = x->Next(level);
            int cmp = (next == nullptr || next == last_bigger) ? 1 : next->key_->compare(key);
            if(cmp == 0 || (cmp > 0 && level ==0)){
                return next;
            }else if(cmp < 0){
                x = next;
            }else{
                last_bigger = next;
                level--;
            }
        }

    }

    void persistent_SkipList::Print() const {
        int i = 0;
        persistent_ptr<Node> start = head_->Next(0);
        while(start != nullptr){
            printf("get:%d %s\n", i++, start->key_->data());
            start = start->Next(0);
        }

    }

    class PersistentSkiplistWrapper {
    public:
        PersistentSkiplistWrapper();

        ~PersistentSkiplistWrapper();

        void Insert(pool_base &pop, const std::string& key);

        void Print();

        void Init(pool_base &pop, int32_t max_height = 12, int32_t branching_factor = 4);

    private:
        //pool<persistent_SkipList> pop_;
        p<bool> been_inited_;
        persistent_ptr<persistent_SkipList> skiplist_;

    };

    PersistentSkiplistWrapper::PersistentSkiplistWrapper() {

    }

    PersistentSkiplistWrapper::~PersistentSkiplistWrapper() {
    }

    void PersistentSkiplistWrapper::Init(pool_base &pop, int32_t max_height, int32_t branching_factor) {
        printf("%d\n", been_inited_);
        transaction::run(pop, [&]{
            if(!been_inited_){
                skiplist_ = make_persistent<persistent_SkipList>(pop, max_height, branching_factor);
                been_inited_ = true;
            }
        });
    }

    void PersistentSkiplistWrapper::Insert(pool_base &pop, const std::string &key) {
        skiplist_->Insert(pop, key.c_str());
    }

    void PersistentSkiplistWrapper::Print() {
        skiplist_->Print();
    }
}; // end rocksdb


int main(int argc, char* argv[]){
    std::string path(argv[1]);
    pool<rocksdb::PersistentSkiplistWrapper> pop;
    if (rocksdb::file_exists(path.c_str()) != 0) {
        pop = pool<rocksdb::PersistentSkiplistWrapper>::create(path.c_str(), "layout", uint64_t(1024*1024*1024), CREATE_MODE_RW);
    } else {
        pop = pool<rocksdb::PersistentSkiplistWrapper>::open(path.c_str(), "layout");
    }

    persistent_ptr<rocksdb::PersistentSkiplistWrapper> skiplist = pop.root();

    skiplist->Init(pop, 12, 4);

    skiplist->Print();

    auto rnd = rocksdb::Random::GetTLSInstance();
    time_t startt, endt;
    startt = clock();
    for(int i = 0; i < 10000 ; i++){
        auto number = rnd->Next();
        char buf[4096];
        sprintf(buf, "%4095d", number);
        skiplist->Insert(pop, buf);
    }
    endt = clock();
    printf("insert cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);
    startt = clock();
    skiplist->Print();
    endt = clock();
    printf("print cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);

    pop.close();

    return 0;
}
