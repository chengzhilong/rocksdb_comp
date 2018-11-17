#ifndef PERSISTENT_HASH_MAP_H
#define PERSISTENT_HASH_MAP_H

#include <stdint.h>

namespace pstl {

class Cobj
{
public:
  virtual uint64_t hashCode() = 0;
  virtual bool equals(Cobj o) {
    return this == o;
  }
//  static uint64_t hashCode(Cobj o) {
//    return o != nullptr ? o.hashCode() : 0;
//  }
};

template<class K, class V>
class Node;

template<class K, class V>
class PersistentHashMap
{
public:
  PersistentHashMap();

  /**
   * The default initial capacity - MUST be a power of two.
   */
  static const int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16


  /**
   * The load factor used when none specified in constructor.
   */
  static const float DEFAULT_LOAD_FACTOR = 0.75f;





  /* ---------------- Static utilities -------------- */
  /**
  * Computes key.hashCode() and spreads (XORs) higher bits of hash
  * to lower.  Because the table uses power-of-two masking, sets of
  * hashes that vary only in bits above the current mask will
  * always collide. (Among known examples are sets of Float keys
  * holding consecutive whole numbers in small tables.)  So we
  * apply a transform that spreads the impact of higher bits
  * downward. There is a tradeoff between speed, utility, and
  * quality of bit-spreading. Because many common sets of hashes
  * are already reasonably distributed (so don't benefit from
  * spreading), and because we use trees to handle large sets of
  * collisions in bins, we just XOR some shifted bits in the
  * cheapest possible way to reduce systematic lossage, as well as
  * to incorporate impact of the highest bits that would otherwise
  * never be used in index calculations because of table bounds.
  */
  static const uint64_t hash(Cobj key) {
    uint64_t h;
    return (key == nullptr) ? 0 : (h = key.hashCode()) ^ (h >> 32);
  }

  /**
  * Returns a power of two size for the given target capacity.
  */
  static const int tableSizeFor(int cap);

  /* ---------------- Fields -------------- */
  Node<K, V> tab_[];
  int threshold_;
  const float loadFactor_;

  /* ---------------- Public operations -------------- */
  PersistentHashMap(int initialCapacity, float loadFactor = 0.75f);
  V get(Cobj key);
  V put(K key, V value);
};

} // end of namespace pstl

#endif // PERSISTENT_HASH_MAP_H
