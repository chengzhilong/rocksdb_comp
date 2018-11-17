//
// Created by 张艺文 on 2018/11/13.
//
//
// Created by 张艺文 on 2018/11/5.
//

#pragma once
#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

#include <cstring>
#include <string>
#include <io.h>
#include <ctime>
#include <fcntl.h>
#include "util/random.h"
#include "util/allocator.h"
#include "libpmem.h"


namespace rocksdb {

    const int kMaxHeight = 12;

    class PersistentAllocator:Allocator{
    public:
        PersistentAllocator(const std::string path, size_t size){
            pmemaddr_ = static_cast<char*>(pmem_map_file(path.c_str(),
                    size, PMEM_FILE_CREATE|PMEM_FILE_EXCL, 0666, &mapped_len_, &is_pmem_));
            if(pmemaddr_ == NULL){
                printf("map error\n");
                exit(-1);
            }
            capacity_ = size;
            now_ = pmemaddr_;
        }

        ~PersistentAllocator(){
            pmem_unmap(pmemaddr_, mapped_len_);
        }

        char* Allocate(size_t bytes) override{
            char* result = now_;
            now_ += bytes;
            return result;
        }

        char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                                    Logger* logger = nullptr) override{
            char* result = now_;
            now_ += bytes;
            return result;
        }

        size_t BlockSize() const override{
            return 0;
        }

    private:
        char* pmemaddr_;
        size_t mapped_len_;
        size_t capacity_;
        int is_pmem_;
        char* now_;
    };

    struct Node {
        explicit Node(const char* key):key_(key) {
        };

        ~Node() = default;

        Node* Next(int n) {
            assert(n >= 0);
            return next_[n];
        }

        void SetNext(int n, Node* next) {
            assert(n >= 0);
            //printf("next_[%d] @ [%p]\n", n, next_[n]);
            next_[n] = next;
            pmem_persist(next_ + n, sizeof(Node*));
        };

        const char* key_;
        Node* next_[1];
    };

    class persistent_SkipList {
    public:
        explicit persistent_SkipList(PersistentAllocator* allocator, int32_t max_height = 12, int32_t branching_factor = 4);

        ~persistent_SkipList(){}

        void Insert(const char *key);

        void Print() const;

        //bool Contains(const char *key);

    private:
        PersistentAllocator* allocator_;
        Node* head_;
        Node** prev_;
        uint32_t prev_height_;
        uint16_t kMaxHeight_;
        uint16_t kBranching_;
        uint32_t kScaledInverseBranching_;

        uint16_t max_height_;


        inline int GetMaxHeight() const {
            return max_height_;
        }

        Node* NewNode(const std::string &key, int height);

        int RandomHeight();

        bool Equal(const char *a, const char *b) {
            return strcmp(a, b) == 0;
        }

        bool LessThan(const char *a, const char *b) {
            return strcmp(a, b) < 0;
        }

        bool KeyIsAfterNode(const std::string& key, Node* n) const;

        Node* FindGreaterOrEqual(const std::string& key) const;

        Node* FindLessThan(const std::string& key, Node** prev = nullptr) const;

        //persistent_ptr<Node> FindLast() const;


    };


    persistent_SkipList::persistent_SkipList(PersistentAllocator* allocator, int32_t max_height, int32_t branching_factor)
            :
            allocator_(allocator),
            kMaxHeight_(static_cast<uint16_t>(max_height)),
            kBranching_(static_cast<uint16_t>(branching_factor)),
            kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),

            max_height_(1) {
        head_ = NewNode(" ", max_height);

        prev_ = reinterpret_cast<Node**>(allocator_->Allocate(sizeof(Node*) * kMaxHeight_));
        for (int i = 0; i < kMaxHeight_; i++) {
            head_->SetNext(i, nullptr);
            prev_[i] = head_;
            pmem_flush(prev_ + i, sizeof(Node*));
        }
        pmem_drain();
        prev_height_ = 1;
    }

    Node* persistent_SkipList::NewNode(const std::string &key, int height) {
        char* mem = allocator_->Allocate(sizeof(Node) + sizeof(Node*) * (height - 1));
        char* pkey = allocator_->Allocate(key.size());
        pmem_memcpy_persist(pkey, key.c_str(), key.size());
        return new (mem) Node(pkey);
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
    bool persistent_SkipList::KeyIsAfterNode(const std::string& key, Node* n) const {
        printf("n is %s\n", n == nullptr?"null":"not null");
        return (n != nullptr) && (strcmp(n->key_, key.c_str()));
    }

    Node* persistent_SkipList::FindLessThan(const std::string &key, Node** prev) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        Node* last_not_after;
        while(true){
            Node* next = x->Next(level);
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

    void persistent_SkipList::Insert(const char *key) {
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

        Node* x = NewNode(key, height);
        for(int i = 0; i < height; i++){
            x->SetNext(i, prev_[i]->Next(i));
            prev_[i]->SetNext(i ,x);
            pmem_persist(prev_[i], sizeof(Node*));
        }
        prev_[0] = x;
        pmem_persist(prev_[0], sizeof(Node*));
        prev_height_ = static_cast<uint16_t >(height);

    }


    Node* persistent_SkipList::FindGreaterOrEqual(const std::string &key) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        Node* last_bigger;
        while(true){
            Node* next = x->Next(level);
            int cmp = (next == nullptr || next == last_bigger) ? 1 : strcmp(next->key_, key.c_str());
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
        Node* start = head_->Next(0);
        while(start != nullptr){
            printf("get:%d %s\n", i++, start->key_);
            start = start->Next(0);
        }

    }
}; // end rocksdb


int main(int argc, char* argv[]){
    std::string path(argv[1]);
    auto allocator = new rocksdb::PersistentAllocator(path, 4ul * 1024 * 1024 * 1024);

    auto skiplist = new rocksdb::persistent_SkipList(allocator, 12, 4);

    auto rnd = rocksdb::Random::GetTLSInstance();
    time_t startt, endt;
    startt = clock();
    for(int i = 0; i < 10000 ; i++){
        auto number = rnd->Next();
        char buf[4096];
        sprintf(buf, "%4095d", number);
        skiplist->Insert(buf);
    }
    endt = clock();
    printf("insert cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);
    startt = clock();
    skiplist->Print();
    endt = clock();
    printf("print cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);

    return 0;
}



