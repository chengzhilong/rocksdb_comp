#include "persistent_hash_map.h"

namespace pstl {

template<class K, class V>
class Node {
public:
  const uint64_t hash_;
  const K key_;
  V value_;
  Node<K, V> *next_;

  Node(uint64_t hash, K key, V value, Node<K, V> *next) {
    hash_ = hash;
    key_ = key;
    value_ = value;
    next_ = next;
  }
  K getKey() final { return key_;}
  V getValue() final { return value_; }
//    const std::string toString() { return key_ + "=" + value_; }
  uint64_t hashCode() final { key_.hashCode() ^ value_.hashCode(); }
  V setValue(V newVal) final {
    V oldVal = value_;
    value_ = newVal;
    return oldVal;
  }
  bool equals(Node<K, V> *o) final {
    if (o == this)
      return true;
    // instanceof
    return key_.equals(o->getKey()) && value_.equals(o->getValue());
  }
};

const int PersistentHashMap::tableSizeFor(int cap)
{
    int n = cap - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
}

PersistentHashMap::PersistentHashMap(int initialCapacity, float loadFactor = 0.75f)
{
  loadFactor_ = loadFactor;
  threshold_ = tableSizeFor(initialCapacity);
}

template<class K, class V>
V PersistentHashMap::get(Cobj key)
{

}

template<class K, class V>
V PersistentHashMap::put(K key, V value)
{
  uint64_t _hash = hash(key);

}

} // end of namespace pstl


