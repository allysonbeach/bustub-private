//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.h
//
// Identification: src/include/primer/hyperloglog.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

/** @brief Capacity of the bitset stream. */
#define BITSET_CAPACITY 64

namespace bustub {

template <typename KeyType>
class HyperLogLog {
  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disable default constructor. */
  HyperLogLog() = delete;

  explicit HyperLogLog(int16_t n_bits) : b_(n_bits), m_(1ULL << n_bits), registers_(m_, 0){};

  /**
   * @brief Getter value for cardinality.
   *
   * @returns cardinality value
   */
  auto GetCardinality() { return cardinality_; }

  auto AddElem(KeyType val) -> void;

  auto ComputeCardinality() -> void;

 private:
  /**
   * @brief Calculates Hash of a given value.
   *
   * @param[in] val - value
   * @returns hash integer of given input value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
    } else {
      val_obj = Value(BIGINT, val);
    }
    return bustub::HashUtil::HashValue(&val_obj);
  }

  /**
   * @brief Gets the register index value from the inital bits.
   *
   * @param[in] bits - the original bitset from the hash of size BITSET_CAPACITY
   * @param[in] b - the size of the intial bits that will be used to determine the register index
   */
  template <size_t N>
  size_t ExtractInitialBits(const std::bitset<N> &bits, size_t b) {
    size_t register_index = 0;
    for (size_t i = 0; i < b; ++i) {
      register_index <<= 1;
      if (bits[N - 1 - i]) {  // indexing from MSB to LSB
        register_index |= 1;
      }
    }
    return register_index;
  }

  auto ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY>;

  auto CalculateNumberOfLeadingZeroes(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t;

  /** @brief Cardinality value. */
  size_t cardinality_ = 0;

  int16_t b_;  // number of bits for register index
  size_t m_;   // number of registers = 2^b_

  /** Register that stores the max leading zero count (p) per register */
  std::vector<uint8_t> registers_;

  /** @todo (student) can add their data structures that support HyperLogLog */
};

}  // namespace bustub
