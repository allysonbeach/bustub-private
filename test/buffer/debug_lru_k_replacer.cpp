#include <iostream>
#include "buffer/lru_k_replacer.h"

using namespace bustub;

int main() {
  LRUKReplacer replacer(10, 2);
  replacer.RecordAccess(1);
  replacer.SetEvictable(1, true);
  auto evicted = replacer.Evict();
  if (evicted.has_value()) {
    std::cout << "Evicted frame: " << evicted.value() << std::endl;
  } else {
    std::cout << "Nothing evicted" << std::endl;
  }
  return 0;
}
