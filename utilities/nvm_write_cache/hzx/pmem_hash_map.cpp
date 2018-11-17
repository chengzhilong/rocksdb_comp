#include "pmem_hash_map.h"

//#include "city.h"
// https://blog.csdn.net/yfkiss/article/details/7337382

namespace p_range {

pmem_hash_map::pmem_hash_map()
{

}

persistent_ptr<char[]> pmem_hash_map::get(const std::string &key, size_t prefixLen)
{
  p_node nod;

//  uint64_t _hash = CityHash64WithSeed(key, prefixLen, 16);
//  nod = getNode(_hash, key);
  node = getNode(key, prefixLen);

  return nod == nullptr ? nullptr : nod->buf;
  // TODO
  // bufSize 需要么
}

p_node pmem_hash_map::getNode(const std::string &key, size_t prefixLen)
{
  uint64_t _hash = CityHash64WithSeed(key, prefixLen, 16);
  return getNode(_hash, key);
}

p_node pmem_hash_map::getNode(uint64_t hash, const std::string &key)
{
  p_node nod = tab[hash % tabLen];

  p_node tmp = nod;
  while (tmp != nullptr) {
    if (tmp->hash_ == hash
        && strcmp(tmp->prefix_, key.c_str()) == 0)
      break;
    tmp = tmp->next;
  }
  return tmp;
}

uint64_t pmem_hash_map::put(pool_base &pop, const std::string &prefix,
                            size_t bufSize) {
  // ( const void * key, int len, unsigned int seed );
  uint64_t _hash = CityHash64WithSeed(prefix, prefix.size(), 16);

  p_node nod = tab[_hash % tabLen];

  p_node tmp = nod;
  while (tmp != nullptr) {
    if (tmp->hash_ == _hash
        && strcmp(tmp->prefix_, prefix.c_str()) == 0)
      break;
    tmp = tmp->next;
  }

  // 前缀没有被插入过
  if (nullptr == tmp) {
    transaction::run(pop, [&] {
      p_node newhead = make_persistent<p_node>();

      newhead->hash_ = _hash;
      newhead->prefixLen = prefix.size();
      newhead->prefix_ = make_persistent<char[]>(prefix.size()+1);
      memcpy(newhead->prefix_, prefix.c_str(), prefix.size()+1);

      // TODO
      // buf = ?  range 分配多大空间
      newhead->bufSize = bufSize
      newhead->buf = make_persistent<char[]>(bufSize);

      newhead->next = nod;
      tab[_hash % tabLen] = newhead;
    });
  }
  else {
    // 已经插入过了
  }
  return _hash;
}

} // end of namespace p_range
