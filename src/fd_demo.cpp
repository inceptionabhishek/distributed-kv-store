#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include "failure_detector.hpp"

int main() {
    std::vector<std::string> nodes = {
        "localhost:50051", "localhost:50052", "localhost:50053", "localhost:50054"
    };

    FailureDetector fd(nodes, std::chrono::milliseconds(300), std::chrono::milliseconds(200));
    fd.Start();

    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::cout << "--- tick " << i << " ---" << std::endl;
        fd.PrintStatus();
    }

    fd.Stop();
    return 0;
}