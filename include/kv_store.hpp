#pragma once

#include <string>
#include <unordered_map>
#include <optional>
#include <mutex>
#include <cstdint>

// A value paired with the timestamp it was written at. Needed so that
// when a client (the router) reads from multiple replicas that might
// disagree, it can tell which one is actually the most recent write.
struct TimestampedValue {
    std::string value;
    uint64_t timestamp;
};

// Stage 6: single-node store, now timestamp-aware for last-write-wins
// conflict resolution across replicas.
class KVStore {
public:
    KVStore() = default;

    // Stores (value, timestamp) for key -- but ONLY if timestamp is >=
    // whatever is currently stored for that key. This is what makes the
    // node itself last-write-wins-safe: if writes to the same key arrive
    // out of order, the older one is silently dropped instead of
    // clobbering the newer value.
    void put(const std::string& key, const std::string& value, uint64_t timestamp);

    std::optional<TimestampedValue> get(const std::string& key) const;

    bool remove(const std::string& key);

    size_t size() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TimestampedValue> data_;
};