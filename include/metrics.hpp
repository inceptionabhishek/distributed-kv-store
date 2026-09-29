#pragma once
#include <array>
#include <atomic>
#include <chrono>
#include <sstream>

struct Metrics {
    std::atomic<uint64_t> reads{0}, writes{0}, failures{0}, rpc_failures{0}, timeouts{0};
    std::atomic<uint64_t> repairs{0}, hints_replayed{0}, rejected{0}, conflicts{0}, migrations{0};
    std::array<std::atomic<uint64_t>, 8> latency{};
    std::atomic<uint64_t> latency_count{0}, latency_us{0};
    inline static constexpr std::array<double, 8> buckets{.0001, .0005, .001, .005, .01, .05, .1, 1.0};
    void Observe(std::chrono::steady_clock::time_point start) {
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start).count();
        latency_us += us; ++latency_count;
        for (size_t i = 0; i < buckets.size(); ++i) if (us / 1e6 <= buckets[i]) ++latency[i];
    }
    std::string Text() const {
        std::ostringstream s;
        s << "# TYPE kv_requests_total counter\n"
          << "kv_requests_total{operation=\"get\"} " << reads << "\n"
          << "kv_requests_total{operation=\"mutate\"} " << writes << "\n"
          << "kv_quorum_failures_total " << failures << "\n"
          << "kv_replica_rpc_failures_total " << rpc_failures << "\n"
          << "kv_deadline_exceeded_total " << timeouts << "\n"
          << "kv_read_repairs_total " << repairs << "\n"
          << "kv_hints_replayed_total " << hints_replayed << "\n"
          << "kv_queue_rejections_total " << rejected << "\n"
          << "kv_version_conflicts_total " << conflicts << "\n"
          << "kv_membership_changes_total " << migrations << "\n"
          << "# TYPE kv_request_latency_seconds histogram\n";
        for (size_t i = 0; i < buckets.size(); ++i)
            s << "kv_request_latency_seconds_bucket{le=\"" << buckets[i] << "\"} " << latency[i] << "\n";
        s << "kv_request_latency_seconds_bucket{le=\"+Inf\"} " << latency_count << "\n"
          << "kv_request_latency_seconds_sum " << latency_us.load() / 1e6 << "\n"
          << "kv_request_latency_seconds_count " << latency_count << "\n";
        return s.str();
    }
};
