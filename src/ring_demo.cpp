#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include "consistent_hash_ring.hpp"

int main() {
    const int num_keys = 10000;
    std::vector<std::string> keys;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back("key" + std::to_string(i));
    }

    {
        int old_n = 3, new_n = 4;
        int moved = 0;
        for (const auto& key : keys) {
            uint64_t h = fnv1a_hash(key);
            if ((h % old_n) != (h % new_n)) {
                moved++;
            }
        }
        std::cout << "[naive % N]        3 -> 4 nodes: "
                  << moved << " / " << num_keys << " keys moved ("
                  << (100.0 * moved / num_keys) << "%)" << std::endl;
    }

    {
        ConsistentHashRing ring(10);
        ring.AddNode("node0");
        ring.AddNode("node1");
        ring.AddNode("node2");

        std::unordered_map<std::string, std::string> before;
        for (const auto& key : keys) {
            before[key] = ring.GetNodeForKey(key);
        }

        ring.AddNode("node3");

        int moved = 0;
        for (const auto& key : keys) {
            if (ring.GetNodeForKey(key) != before[key]) {
                moved++;
            }
        }
        std::cout << "[consistent hash]  3 -> 4 nodes: "
                  << moved << " / " << num_keys << " keys moved ("
                  << (100.0 * moved / num_keys) << "%)" << std::endl;
        std::cout << "  (ideal minimum would be ~" << (100.0 / 4) << "%, "
                  << "since a new 4th node should take about 1/4 of the keys)" << std::endl;
    }

    return 0;
}