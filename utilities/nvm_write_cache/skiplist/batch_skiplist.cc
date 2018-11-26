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
#include "test_common.h"
#include "libpmemobj++/p.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/make_persistent.hpp"
#include "libpmemobj++/make_persistent_array.hpp"
#include "persistent_batchupdate_skiplist.h"

using namespace pmem::obj;

namespace rocksdb {

    static inline int
    file_exists(char const *file) {
        return access(file, F_OK);
    }

    const int kMaxHeight = 12;

    /*struct p_string{
        persistent_ptr<char[]> data_;
        p<size_t> size_;

        p_string(pool_base& pop, const std::string& src){
            transaction::run(pop, [&]{
                data_ = make_persistent<char[]>(src.size() + 1);
                memcpy(&data_[0], src.c_str(), src.size());
                data_[src.size()] = 0;
                size_ = src.size();
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
    };*/

    class KeyBuffer {
    public:
        KeyBuffer() = default;

        virtual ~KeyBuffer() = default;

        virtual uint64_t Allocate(const char *key, size_t size) = 0;

        virtual char *Get(uint64_t off) = 0;
    };

    class PersistentKeyBuffer : public KeyBuffer {
    public:
        PersistentKeyBuffer(pool_base &pop, size_t size):pop_(pop){
            transaction::run(pop, [&] {
                buf_ = make_persistent<persistent_ptr<char[]>[]>(10000);
            });
            cur_ = 0;
        }

        ~PersistentKeyBuffer() override {

        }

        uint64_t Allocate(const char *key, size_t size) override {
            /*memcpy(&buf_[0] + now_, key, size);
            uint64_t rtn = now_;
            now_ += size;
            return rtn;*/
            transaction::run(pop_, [&] {
                buf_[cur_] = make_persistent<char[]>(size);
                memcpy(&buf_[cur_][0], key, size);
                cur_ = cur_ + 1;
            });
            return size;
        }

        char *Get(uint64_t off) override {
            return &buf_[0] + off;
        }

    private:
        pool_base& pop_;
        persistent_ptr<persistent_ptr<char[]>[]> buf_;
        p<int> cur_;
    };


    struct Node {
        explicit Node() {
        };

        ~Node() = default;

        Node *Next(int n) {
            assert(n >= 0);
            return next_[n];
        }

        void SetNext(int n, Node *next) {
            assert(n >= 0);
            next_[n] = next;
        };

        bool persisted;
        char*  vkey_;
        size_t  key_size;
        Node *next_[1];
    };

    class persistent_SkipList {
    public:
        explicit persistent_SkipList(pool_base &pop, int32_t max_height = 12, int32_t branching_factor = 4);

        ~persistent_SkipList() = default;

        void Insert(pool_base &pop, const char *key);

        void Print() const;

        //bool Contains(const char *key);

    private:
        Node *head_;
        Node **prev_;
        uint32_t prev_height_;
        uint16_t kMaxHeight_;
        uint16_t kBranching_;
        uint32_t kScaledInverseBranching_;

        uint16_t max_height_;

        PersistentKeyBuffer* pbuffer_;
        uint64_t count_;


        inline int GetMaxHeight() const {
            return max_height_;
        }

        Node *NewNode(pool_base &pop, const std::string &key, int height);

        int RandomHeight();

        bool Equal(const char *a, const char *b) {
            return strcmp(a, b) == 0;
        }

        bool LessThan(const char *a, const char *b) {
            return strcmp(a, b) < 0;
        }

        bool KeyIsAfterNode(const std::string &key, Node *n) const;

        Node *FindGreaterOrEqual(const std::string &key) const;

        Node *FindLessThan(const std::string &key, Node **prev = nullptr) const;

        void DoPersist();
        //persistent_ptr<Node> FindLast() const;


    };


    persistent_SkipList::persistent_SkipList(pool_base &pop, int32_t max_height, int32_t branching_factor)
            :
            kMaxHeight_(static_cast<uint16_t>(max_height)),
            kBranching_(static_cast<uint16_t>(branching_factor)),
            kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),
            max_height_(1),
            pbuffer_(new PersistentKeyBuffer(pop, 1ul * 1024 * 1024 * 1024)),
            count_(0){
        head_ = NewNode(pop, " ", max_height);
        prev_ = static_cast<Node **>(malloc(sizeof(Node *) * max_height));

        for (int i = 0; i < kMaxHeight_; i++) {
            head_->SetNext(i, nullptr);
            prev_[i] = head_;
        }

        prev_height_ = 1;

    }

    Node *persistent_SkipList::NewNode(pool_base &pop, const std::string &key, int height) {
        Node *n;
        n = static_cast<Node*>(malloc(sizeof(Node) + height * sizeof(Node*)));
        n->vkey_ = new char[key.size()];
        memcpy(n->vkey_, key.c_str(), key.size());
        n->key_size = key.size();
        n->persisted = false;
        count_+= n->key_size;
        if(count_ > 4096){
            DoPersist();
            count_ = 0;
        }
        return n;
    }

    void persistent_SkipList::DoPersist() {
        Node *start = head_->Next(0);
        std::string tmp;
        while (start != nullptr) {
            if(!start->persisted){
                tmp.append(start->vkey_, start->key_size);
                start->persisted = true;
            }
            start = start->Next(0);
        }
        pbuffer_->Allocate(tmp.c_str(), tmp.size());

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
    bool persistent_SkipList::KeyIsAfterNode(const std::string &key, Node *n) const {
        //printf("n is %s\n", n == nullptr ? "null" : "not null");
        return (n != nullptr) && (strcmp(n->vkey_, key.c_str()) < 0);
    }

    Node *persistent_SkipList::FindLessThan(const std::string &key,
                                            Node **prev) const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        Node *last_not_after;
        while (true) {
            Node *next = x->Next(level);
            if (next != last_not_after && KeyIsAfterNode(key, next)) {
                x = next;
            } else {
                prev[level] = x;
                if (level == 0) {
                    return x;
                } else {
                    last_not_after = next;
                    level--;
                }
            }
        }
    }

    void persistent_SkipList::Insert(pool_base &pop, const char *key) {
        // key < prev[0]->next(0) && prev[0] is head or key < prev[0]
        if (!KeyIsAfterNode(key, prev_[0]->Next(0)) &&
            (prev_[0] == head_ || KeyIsAfterNode(key, prev_[0]))) {
            for (uint32_t i = 1; i < prev_height_; i++) {
                prev_[i] = prev_[0];
            }
        } else {
            FindLessThan(key, prev_);
        }

        int height = RandomHeight();
        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; i++) {
                prev_[i] = head_;
            }
            max_height_ = static_cast<uint16_t >(height);
        }

        Node *x = NewNode(pop, key, height);
        for (int i = 0; i < height; i++) {
            x->SetNext(i, prev_[i]->Next(i));
            prev_[i]->SetNext(i, x);
        }
        prev_[0] = x;
        prev_height_ = static_cast<uint16_t >(height);

    }


    Node *persistent_SkipList::FindGreaterOrEqual(const std::string &key) const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        Node *last_bigger;
        while (true) {
            Node *next = x->Next(level);
            int cmp = (next == nullptr || next == last_bigger) ? 1 : strcmp(next->vkey_, key.c_str());
            if (cmp == 0 || (cmp > 0 && level == 0)) {
                return next;
            } else if (cmp < 0) {
                x = next;
            } else {
                last_bigger = next;
                level--;
            }
        }

    }

    void persistent_SkipList::Print() const {
        Node *start = head_->Next(0);
        while (start != nullptr) {
            start = start->Next(0);
        }

    }

    class PersistentSkiplistWrapper {
    public:
        PersistentSkiplistWrapper();

        ~PersistentSkiplistWrapper();

        void Insert(pool_base &pop, const std::string &key);

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
        transaction::run(pop, [&] {
            if (!been_inited_) {
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


int main(int argc, char *argv[]) {
    std::string path(argv[1]);
    pool<rocksdb::PersistentSkiplistWrapper> pop;
    if (rocksdb::file_exists(path.c_str()) != 0) {
        pop = pool<rocksdb::PersistentSkiplistWrapper>::create(path.c_str(), "layout", 4ul * 1024 * 1024 * 1024),
                                                               CREATE_MODE_RW);
    } else {
        pop = pool<rocksdb::PersistentSkiplistWrapper>::open(path.c_str(), "layout");
    }

    persistent_ptr<rocksdb::PersistentSkiplistWrapper> skiplist = pop.root();

    skiplist->Init(pop, 12, 4);

    skiplist->Print();

    auto rnd = rocksdb::Random::GetTLSInstance();
    time_t startt, endt;
    startt = clock();
    for (int i = 0; i < 10000; i++) {
        auto number = rnd->Next();
        char buf[4096];
        sprintf(buf, "%4095d", number);
        skiplist->Insert(pop, buf);
    }
    endt = clock();
    printf("insert cost  = %f\n", (double) (endt - startt) / CLOCKS_PER_SEC);
    /*startt = clock();
    skiplist->Print();
    endt = clock();
    printf("print cost  = %f\n", (double) (endt - startt) / CLOCKS_PER_SEC);*/

    pop.close();

    return 0;
}
