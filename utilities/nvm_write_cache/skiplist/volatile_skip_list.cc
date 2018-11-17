//
// Created by 张艺文 on 2018/11/5.
//

#include <time.h>
#include <cstring>
#include <string>
#include <util/random.h>
#include <cassert>
#include <fcntl.h>


namespace rocksdb {


    const int kMaxHeight = 12;

    struct Node {
        explicit Node(const std::string &key, int height)
                :
                key_(key){
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
        Node* next_[kMaxHeight];
    };

    class volatile_SkipList {
    public:
        explicit volatile_SkipList(int32_t max_height = 12, int32_t branching_factor = 4);

        ~volatile_SkipList() =default;

        void Insert(const char *key);

        void Print() const;

        //bool Contains(const char *key);

    private:
        Node* head_;
        Node* prev_[kMaxHeight];
        uint32_t prev_height_;
        uint16_t kMaxHeight_;
        uint16_t kBranching_;
        uint32_t kScaledInverseBranching_;

        uint16_t max_height_;

        int fd_;


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

        Node* FindLessThan(const std::string& key, Node *prev[]) const;

        //persistent_ptr<Node> FindLast() const;


    };


    volatile_SkipList::volatile_SkipList(int32_t max_height, int32_t branching_factor)
            :
            kMaxHeight_(static_cast<uint16_t>(max_height)),
            kBranching_(static_cast<uint16_t>(branching_factor)),
            kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),

            max_height_(1) {

        head_ = NewNode("", max_height_);

        //Node* v_head = pmemobj_direct(head_.raw());
        for (int i = 0; i < kMaxHeight_; i++) {
            head_->SetNext(i, nullptr);
            prev_[i] = head_;
        }

        prev_height_ = 1;

        fd_ = open("/tmp/skiplistlog", O_CREAT|O_SYNC|O_RDWR);
    }

    Node* volatile_SkipList::NewNode(const std::string &key, int height) {
        Node* n;
        n = new Node(key, height);
        write(fd, key.c_str(), key.size());
        return n;
    }

    int volatile_SkipList::RandomHeight() {
        auto rnd = Random::GetTLSInstance();
        int height = 1;
        while (height < kMaxHeight_ && rnd->Next() < kScaledInverseBranching_) {
            height++;
        }
        return height;
    }

    // when n < key returns true
    // n should be at behind of key means key is after node
    bool volatile_SkipList::KeyIsAfterNode(const std::string& key, Node* n) const {
        return (n != nullptr) && (n->key_.compare(key));
    }

    Node* volatile_SkipList::FindLessThan(const std::string &key,
                                          Node* prev[]) const {
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

    void volatile_SkipList::Insert(const char *key) {
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
        }
        prev_[0] = x;
        prev_height_ = static_cast<uint16_t >(height);

    }


    Node* volatile_SkipList::FindGreaterOrEqual(const std::string &key) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        Node* last_bigger;
        while(true){
            Node* next = x->Next(level);
            int cmp = (next == nullptr || next == last_bigger) ? 1 : next->key_.compare(key);
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

    void volatile_SkipList::Print() const {
        int i = 0;
        Node* start = head_;
        while(start->Next(0) != nullptr){
            printf("get:%d %s\n", i++, start->key_.c_str());
            start = start->Next(0);
        }
    }
} // end rocksdb


int main(){
    auto skiplist = new rocksdb::volatile_SkipList(12, 4);
    auto rnd = rocksdb::Random::GetTLSInstance();
    time_t startt, endt;
    startt = clock();
    for(int i = 0; i < 1000 ; i++){
        auto number = rnd->Next();
        char buf[16];
        sprintf(buf, "%15d", number);
        printf("Insert %s\n", buf);
        skiplist->Insert(buf);
    }
    endt = clock();
    printf("cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);

    startt = clock();
    skiplist->Print();
    endt = clock();
    printf("cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);
    return 0;
}
