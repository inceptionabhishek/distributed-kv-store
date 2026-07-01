#pragma once

#include <string>
#include <unordered_map>
#include <optional>

// Stage 1: single-node key-value store.
// Not thread-safe yet -- that's the next step.
class KVStore {
public:
    KVStore() = default;

    // Insert or overwrite the value for a key.
    void put(const std::string& key, const std::string& value);

    // Return the value for a key, or std::nullopt if it doesn't exist.
    // No exceptions for the "miss" case -- misses are normal, not exceptional.
    std::optional<std::string> get(const std::string& key) const;

    // Remove a key. Returns true if it existed and was removed,
    // false if it was never there.
    bool remove(const std::string& key);

    // Useful for tests/debugging.
    size_t size() const;

private:
    std::unordered_map<std::string, std::string> data_;
};