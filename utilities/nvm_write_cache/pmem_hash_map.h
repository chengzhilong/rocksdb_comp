#ifndef PMEM_HASH_MAP_H
#define PMEM_HASH_MAP_H

#include <string>
#include <vector>

#include "libpmemobj++/p.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/make_persistent.hpp"
#include "libpmemobj++/make_persistent_array.hpp"

using namespace pmem::obj;
namespace p_range {
using std::string;
using std::vector;

/*struct Node {
  p<uint64_t> hash_;
  persistent_ptr<Node> next;
  p<size_t> prefixLen; // string prefix_ tail 0 not included
  persistent_ptr<char[]> prefix_;
  persistent_ptr<char[]> key_range_;
  p<size_t> chunk_num_;
  p<uint64_t> seq_num_;

  p<size_t> bufSize; // capacity
  persistent_ptr<char[]> buf;
  p<size_t> dataLen; // exact data len
};*/

//using p_node = persistent_ptr<Node>;

template<typename T>
class pmem_hash_map {
private:
    struct Node2 {
        persistent_ptr<Node2> next;
        persistent_ptr<T> p_content;
    };

    using p_node_t = persistent_ptr<Node2>;

    p<uint32_t> tabLen_;
    persistent_ptr<p_node_t[]> tab_;

    p<float> loadFactor_;
    p<uint32_t> threshold_;
    p<uint32_t> size_;

public:
    pmem_hash_map(pool_base& pop, double loadFactor, uint64_t tabLen);

    void getAll(vector<persistent_ptr<T> > &nodeVec);

    void put(pool_base &pop, persistent_ptr<T> &p_content);

    //persistent_ptr<char[]> get(const std::string &key, size_t prefixLen);

    //p_node_t getNode(const std::string &key, size_t prefixLen);

    //p_node_t getNode(uint64_t hash, const std::string &key);

    //uint64_t put(pool_base &pop, const string &prefix, size_t bufSize);

    //p_node_t putAndGet(pool_base &pop, const string &prefix, size_t bufSize);
};

template <typename T>
pmem_hash_map<T>::pmem_hash_map(pool_base &pop, double loadFactor, uint64_t tabLen) {
    transaction::run(pop, [&]{
        tabLen_ = tabLen;
        tab_ = make_persistent<p_node_t[]>(tabLen);
        loadFactor_ = loadFactor;
        threshold_ = tabLen_ * loadFactor_;
        size_ = 0;
    });
}

template <typename T>
void pmem_hash_map<T>::getAll(std::vector<pmem::obj::persistent_ptr<T>> &nodeVec) {
    size_t tablen = tabLen_;
    for (size_t i = 0; i < tablen; ++i) {
        p_node_t node = tab_[i];

        while (node != nullptr) {
            nodeVec.push_back(node->p_content);
            node = node->next;
        }
    }
}

template <typename T>
void pmem_hash_map<T>::put(pool_base &pop, persistent_ptr<T> &p_content) {
    // 调用者自己构建 map ，检查是否已经有同样的 key
    uint64_t _hash = p_content->hashCode();
    p_node_t bucketHeadNode = tab_[_hash % tabLen_];

    p_node_t newhead;
    transaction::run(pop, [&] {
        newhead = make_persistent<p_node_t>();
        newhead->p_content = p_content;
        newhead->next = nullptr;
        //tab_[_hash % tabLen_] = newhead;
    });

    if (nullptr == bucketHeadNode) {
         bucketHeadNode = newhead;
    }else{
        newhead -> next = bucketHeadNode->next;
        bucketHeadNode->next = newhead;
    }
}

/*template <typename T>
uint64_t pmem_hash_map<T>::put(pool_base &pop, const string &prefix, size_t bufSize) {
    return putAndGet(pop, prefix, bufSize)->hash_;
}*/

/*template <typename T>
p_node_t pmem_hash_map<T>::getNode(uint64_t hash, const std::string &key) {
    p_node_t nod = tab_[hash % tabLen_];

    p_node_t tmp = nod;
    while (tmp != nullptr) {
        if (tmp->hash_ == hash
            && strcmp(tmp->prefix_, key.c_str()) == 0)
            break;
        tmp = tmp->next;
    }
    return tmp;
}*/

/*template <typename T>
p_node_t pmem_hash_map<T>::getNode(const string &key, size_t prefixLen) {
    uint64_t _hash = CityHash64WithSeed(key, prefixLen, 16);
    return getNode(_hash, key);
}*/

/*template <typename T>
persistent_ptr<char[]> pmem_hash_map<T>::get(const std::string &key, size_t prefixLen) {
    p_node_t node;

//  uint64_t _hash = CityHash64WithSeed(key, prefixLen, 16);
//  nod = getNode(_hash, key);
    node = getNode(key, prefixLen);

    return node == nullptr ? nullptr : node->buf;
    // TODO
    // bufSize 需要么
}*/

/*template <typename T>
p_node_t pmem_hash_map<T>::putAndGet(pool_base &pop, const string &prefix, size_t bufSize) {
    // ( const void * key, int len, unsigned int seed );
    uint64_t _hash = CityHash64WithSeed(prefix, prefix.length(), 16);

    p_node_t bucketHeadNode = tab_[_hash % tabLen_];

    p_node_t try2find = bucketHeadNode;
    while (try2find != nullptr) {
        if (try2find->hash_ == _hash
            && strcmp(try2find->prefix_, prefix.c_str()) == 0)
            break;
        try2find = try2find->next;
    }

    p_node_t newhead = try2find;
    // 前缀没有被插入过
    if (nullptr == try2find) {
        transaction::run(pop, [&] {
            newhead = make_persistent<p_node_t>();

            newhead->hash_ = _hash;
            newhead->prefixLen = prefix.length();
            newhead->prefix_ = make_persistent<char[]>(prefix.length() + 1);
            memcpy(newhead->prefix_, prefix.c_str(), prefix.length() + 1);

            // TODO
            // key_range_ ?
            // seq_num_ ?
            newhead->key_range_ = nullptr;
            newhead->chunk_num_ = 0;
            newhead->seq_num_ = 0;

            newhead->bufSize = bufSize;
            newhead->buf = make_persistent<char[]>(bufSize);
            newhead->dataLen = 0;

            newhead->next = bucketHeadNode;
            tab_[_hash % tabLen_] = newhead;
        });
    } else {
        // 已经插入过了
    }
    return newhead;
}*/

} // end of namespace p_range

#endif // PMEM_HASH_MAP_H