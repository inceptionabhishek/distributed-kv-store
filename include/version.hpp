#pragma once
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <tuple>
#include <random>

inline uint64_t WallMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
inline std::string UniqueWriter(const std::string& prefix) {
    std::random_device random;
    return prefix + "-" + std::to_string(WallMillis()) + "-" +
        std::to_string(random()) + "-" + std::to_string(random());
}
struct Version {
    uint64_t physical_ms = 0, logical = 0;
    std::string writer;
    auto tuple() const { return std::tie(physical_ms, logical, writer); }
    bool operator<(const Version& b) const { return tuple() < b.tuple(); }
    bool operator==(const Version& b) const { return tuple() == b.tuple(); }
    bool operator!=(const Version& b) const { return !(*this == b); }
};
class HybridClock {
public:
    explicit HybridClock(std::string writer) : writer_(std::move(writer)) {}
    Version Next() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = WallMillis();
        if (now > physical_) { physical_ = now; logical_ = 0; } else ++logical_;
        return {physical_, logical_, writer_};
    }
    void Observe(const Version& v) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (v.physical_ms > physical_) { physical_ = v.physical_ms; logical_ = v.logical; }
        else if (v.physical_ms == physical_) logical_ = std::max(logical_, v.logical);
    }
private:
    std::mutex mutex_;
    uint64_t physical_ = 0, logical_ = 0;
    std::string writer_;
};
