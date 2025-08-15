//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "include/primer/hyperloglog.h"

namespace bustub {
/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(hash);
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::CalculateNumberOfLeadingZeroes(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  for (int i = BITSET_CAPACITY - 1; i >= 0; --i) {
    if (bset[i]) {
      return BITSET_CAPACITY - i;  // number of leading zeroes + 1
    }
  }
  return BITSET_CAPACITY;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  // Compute the hash of val
  // Compute binary
  // Take the n_bits of the binary to see the register number
  // Then the remainding bits to see what is the number of leading zeros + 1 from PositionOfLeftmostOne to get p
  // register[r] = max(register[r], p)
  const hash_t hash = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> hash_bitset = ComputeBinary(hash);

  // Extract the intial bits and convert to register index
  int register_index = ExtractInitialBits(hash_bitset, b_);

  std::bitset<BITSET_CAPACITY> p_bits;
  for (int i = 0; i < BITSET_CAPACITY - b_; ++i) {
    if (hash_bitset[i]) {
      p_bits.set(i);
    }
  }

  // get value of p
  int p = CalculateNumberOfLeadingZeroes(p_bits);  // do I need to add 1 to this?

  // Cache the max number of leading zeros to the register
  registers_[register_index] = std::max(registers_[register_index], static_cast<uint8_t>(p));
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  m_ = std::size(registers_);  // this should be 2^b
  double sum = 0.0;
  for (uint8_t reg : registers_) {
    sum += 1.0 / (1ULL << reg);  // remember that 1ULL << value is 2^value and 1/(1ULL << value) is 2^-value
  }
  cardinality_ = static_cast<size_t>(CONSTANT * m_ * (m_ / sum));
  // TODO: round to std::floor
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
