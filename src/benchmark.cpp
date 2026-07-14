#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <random>
#include <cstdlib>

#include "router.hpp"

struct ThreadResult {
    std::vector<double> latencies_us;
    long ops_done = 0;
};

int main(int argc, char** argv) {
    int num_threads = 8;
    int duration_seconds = 10;
    int num_keys = 1000;
    double write_ratio = 0.5;

    if (argc > 1) num_threads = std::atoi(argv[1]);
    if (argc > 2) duration_seconds = std::atoi(argv[2]);
    if (argc > 3) num_keys = std::atoi(argv[3]);
    if (argc > 4) write_ratio = std::atof(argv[4]);

    std::vector<std::string> nodes = {
        "localhost:50051", "localhost:50052", "localhost:50053", "localhost:50054"
    };

    std::cout << "Benchmark config: threads=" << num_threads
              << " duration=" << duration_seconds << "s"
              << " keyspace=" << num_keys
              << " write_ratio=" << write_ratio << std::endl;

    Router bench_router(nodes, 10, 3, 2, 2, /*verbose=*/false);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    for (int i = 0; i < num_keys; ++i) {
        bench_router.Put("bench:" + std::to_string(i), "seed_value");
    }
    std::cout << "Pre-populated " << num_keys << " keys." << std::endl;

    std::atomic<bool> stop{false};
    std::vector<ThreadResult> results(num_threads);

    auto worker = [&](int thread_id) {
        std::mt19937 rng(thread_id + 12345);
        std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
        std::uniform_real_distribution<double> op_dist(0.0, 1.0);

        auto& result = results[thread_id];
        while (!stop.load(std::memory_order_relaxed)) {
            std::string key = "bench:" + std::to_string(key_dist(rng));
            bool do_write = op_dist(rng) < write_ratio;

            auto t0 = std::chrono::steady_clock::now();
            if (do_write) {
                bench_router.Put(key, "value_" + std::to_string(rng()));
            } else {
                std::string out;
                bench_router.Get(key, &out);
            }
            auto t1 = std::chrono::steady_clock::now();

            result.latencies_us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
            result.ops_done++;
        }
    };

    std::vector<std::thread> threads;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
    stop.store(true);
    for (auto& t : threads) t.join();

    auto end = std::chrono::steady_clock::now();
    double elapsed_seconds = std::chrono::duration<double>(end - start).count();

    std::vector<double> all_latencies;
    long total_ops = 0;
    for (auto& r : results) {
        total_ops += r.ops_done;
        all_latencies.insert(all_latencies.end(), r.latencies_us.begin(), r.latencies_us.end());
    }
    std::sort(all_latencies.begin(), all_latencies.end());

    auto percentile = [&](double p) -> double {
        if (all_latencies.empty()) return 0.0;
        size_t idx = static_cast<size_t>(p * (all_latencies.size() - 1));
        return all_latencies[idx];
    };

    double throughput = total_ops / elapsed_seconds;

    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "Total ops:      " << total_ops << std::endl;
    std::cout << "Elapsed:        " << elapsed_seconds << "s" << std::endl;
    std::cout << "Throughput:     " << throughput << " ops/sec" << std::endl;
    std::cout << "Latency p50:    " << percentile(0.50) << " us" << std::endl;
    std::cout << "Latency p95:    " << percentile(0.95) << " us" << std::endl;
    std::cout << "Latency p99:    " << percentile(0.99) << " us" << std::endl;
    std::cout << "Latency max:    " << (all_latencies.empty() ? 0.0 : all_latencies.back()) << " us" << std::endl;

    return 0;
}