#include <iostream>
#include "kv_store.hpp"

int main() {
    KVStore store;

    store.put("name", "a");
    store.put("role", "backend engineer");

    // std::optional forces you to check before use -- there's no
    // "garbage value" you could accidentally read, unlike a raw
    // out-param or a sentinel string like "".
    if (auto val = store.get("name")) {
        std::cout << "name -> " << *val << std::endl;
    }

    if (auto val = store.get("role")) {
        std::cout << "role -> " << *val << std::endl;
    }

    std::cout << "size -> " << store.size() << std::endl;

    bool removed = store.remove("role");
    std::cout << "removed role? " << removed << std::endl;
    std::cout << "size after remove -> " << store.size() << std::endl;

    auto missing = store.get("role");
    if (!missing.has_value()) {
        std::cout << "role is a miss, as expected" << std::endl;
    }

    return 0;
}