#include "router.hpp"
#include "runtime.hpp"
#include <algorithm>
#include <atomic>
#include <iostream>
#include <random>
#include <thread>
struct ThreadResult {
    std::vector<double> latencies;
    uint64_t successful = 0, failed = 0, reads = 0, writes = 0;
    uint64_t stale = 0, quorum_failed = 0, conflicts = 0;
    double max_us = 0;
};
int main(int argc, char** argv) {
    try {
        Arguments args(argc, argv);
        auto positional = [&](size_t i, const std::string& fallback) {
            return args.positional.size() > i ? args.positional[i] : fallback;
        };
        int workers = std::stoi(positional(0, "8")), duration = std::stoi(positional(1, "10"));
        int keys = std::stoi(positional(2, "1000")); double ratio = std::stod(positional(3, "0.5"));
        if (workers < 1 || workers > 256 || duration < 1 || keys < 1 || ratio < 0 || ratio > 1)
            throw std::invalid_argument("require threads=1..256, duration>=1, keys>=1, ratio=0..1");
        RouterOptions options; options.sequential = args.Get("mode", "concurrent") == "sequential";
        if (args.Get("mode", "concurrent") != "sequential" && args.Get("mode", "concurrent") != "concurrent")
            throw std::invalid_argument("mode must be concurrent or sequential");
        options.workers = std::max(24, workers * 3);
        options.repair_interval = std::chrono::milliseconds(0); // isolate foreground path; hints still replay
        Router router(SplitNodes(args.Get("nodes", DefaultNodes())), options);
        for (int i = 0; i < keys; ++i)
            if (!router.Put("bench:" + std::to_string(i), "seed")) throw std::runtime_error("prepopulation failed");
        router.Drain();
        std::atomic<bool> stop{false}; std::atomic<int> ready{0}; std::atomic<bool> go{false};
        std::vector<ThreadResult> results(workers); std::vector<std::thread> threads;
        for (int id = 0; id < workers; ++id) threads.emplace_back([&, id] {
            std::mt19937_64 rng(12345 + id), sample_rng(98765 + id);
            std::uniform_int_distribution<int> key_dist(0, keys - 1);
            std::uniform_real_distribution<double> op_dist(0, 1);
            auto& result = results[id]; result.latencies.reserve(100000); ++ready;
            while (!go.load()) std::this_thread::yield();
            while (!stop.load(std::memory_order_relaxed)) {
                auto key = "bench:" + std::to_string(key_dist(rng)); bool write = op_dist(rng) < ratio;
                auto start = std::chrono::steady_clock::now(); bool success;
                if (write) {
                    auto write_result = router.Mutate(router.NewEntry(key, "v" + std::to_string(rng())));
                    success = write_result.success; ++result.writes;
                    if (!success) {
                        if (write_result.status == kvstore::STALE) ++result.stale;
                        else if (write_result.status == kvstore::CONFLICT) ++result.conflicts;
                        else ++result.quorum_failed;
                    }
                } else {
                    auto read = router.Read(key); success = read.status == ReadStatus::Found; ++result.reads;
                    if (read.status == ReadStatus::Unavailable) ++result.quorum_failed;
                    if (read.status == ReadStatus::Conflict) ++result.conflicts;
                }
                double us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count();
                if (!success) { ++result.failed; continue; }
                ++result.successful; result.max_us = std::max(result.max_us, us);
                // Uniform bounded reservoir per worker; percentile values are sample estimates.
                if (result.latencies.size() < 100000) result.latencies.push_back(us);
                else {
                    std::uniform_int_distribution<uint64_t> pick(0, result.successful - 1);
                    auto index = pick(sample_rng); if (index < result.latencies.size()) result.latencies[index] = us;
                }
            }
        });
        while (ready < workers) std::this_thread::yield();
        auto start = std::chrono::steady_clock::now(); go = true;
        std::this_thread::sleep_for(std::chrono::seconds(duration)); stop = true;
        for (auto& thread : threads) thread.join();
        double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        uint64_t success = 0, failed = 0, reads = 0, writes = 0, stale = 0, quorum_failed = 0, conflicts = 0; double max_us = 0;
        std::vector<double> samples;
        for (auto& r : results) {
            success += r.successful; failed += r.failed; reads += r.reads; writes += r.writes;
            stale += r.stale; quorum_failed += r.quorum_failed; conflicts += r.conflicts;
            max_us = std::max(max_us, r.max_us); samples.insert(samples.end(), r.latencies.begin(), r.latencies.end());
        }
        std::sort(samples.begin(), samples.end());
        auto p = [&](double fraction) { return samples.empty() ? 0 : samples[static_cast<size_t>(fraction * (samples.size() - 1))]; };
        std::cout << "{\"mode\":\"" << args.Get("mode", "concurrent") << "\",\"threads\":" << workers
                  << ",\"keyspace\":" << keys << ",\"write_ratio\":" << ratio << ",\"elapsed_seconds\":" << elapsed
                  << ",\"successful\":" << success << ",\"failed\":" << failed << ",\"reads\":" << reads << ",\"writes\":" << writes
                  << ",\"stale_rejections\":" << stale << ",\"quorum_failures\":" << quorum_failed << ",\"conflicts\":" << conflicts
                  << ",\"successful_ops_per_sec\":" << success / elapsed << ",\"attempted_ops_per_sec\":" << (success + failed) / elapsed
                  << ",\"sample_count\":" << samples.size() << ",\"p50_us\":" << p(.5) << ",\"p95_us\":" << p(.95)
                  << ",\"p99_us\":" << p(.99) << ",\"max_us\":" << max_us << "}\n";
        return failed ? 2 : 0;
    } catch (const std::exception& e) { std::cerr << e.what() << "\n"; return 1; }
}
