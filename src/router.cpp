#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>

#include "router.hpp"

int main() {
    std::vector<std::string> nodes = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        "localhost:50054"
    };

    Router router(nodes, /*virtual_nodes=*/10, /*replication_factor=*/3,
                  /*write_quorum=*/2, /*read_quorum=*/2);

    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    std::vector<std::pair<std::string, std::string>> data = {
        {"user:1", "abhishek"},
        {"user:2", "priya"},
        {"user:3", "rahul"},
        {"user:4", "meera"},
        {"user:5", "arjun"},
        {"order:100", "shipped"},
        {"order:101", "pending"},
        {"order:102", "delivered"},
    };

    std::cout << "--- writing ---" << std::endl;
    int put_failures = 0;
    for (const auto& [key, value] : data) {
        if (!router.Put(key, value)) {
            put_failures++;
        }
        router.ReplayHintsForRecoveredNodes();
    }

    std::cout << "\n--- reading back ---" << std::endl;
    int mismatches = 0;
    for (const auto& [key, expected_value] : data) {
        std::string actual_value;
        bool found = router.Get(key, &actual_value);
        if (!found || actual_value != expected_value) {
            std::cout << "MISMATCH on key " << key << std::endl;
            mismatches++;
        }
    }

    std::cout << "\nput_failures=" << put_failures << " mismatches=" << mismatches << std::endl;
    std::cout << (put_failures == 0 && mismatches == 0 ? "PASS" : "FAIL") << std::endl;

    return 0;
}