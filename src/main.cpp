#include <iostream>
#include <thread>
#include <vector>
#include "kv_store.hpp"

int main() {
    KVStore store;

    const int num_threads = 8;
    const int puts_per_thread = 10000;

    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&store, t, puts_per_thread]() {
            for (int i = 0; i < puts_per_thread; ++i) {
                std::string key = "thread" + std::to_string(t) + "_key" + std::to_string(i);
                store.put(key, "value" + std::to_string(i));
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    size_t expected = static_cast<size_t>(num_threads) * puts_per_thread;
    std::cout << "expected size -> " << expected << std::endl;
    std::cout << "actual size   -> " << store.size() << std::endl;

    if (store.size() == expected) {
        std::cout << "PASS: no lost writes under concurrent access" << std::endl;
    } else {
        std::cout << "FAIL: writes were lost" << std::endl;
    }

    return 0;
}