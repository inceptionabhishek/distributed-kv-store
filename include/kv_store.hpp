#pragma once

#include <string>
#include <unordered_map>
#include <optional>
#include <mutex>

// Stage 1: single-node, thread-safe key-value store.
class KVStore {
public:
    KVStore() = default;

    void put(const std::string& key, const std::string& value);

    std::optional<std::string> get(const std::string& key) const;

    bool remove(const std::string& key);

    size_t size() const;

private:
    mutable std::mutex mutex_;   // 'mutable' so we can lock it inside const methods like get()
    std::unordered_map<std::string, std::string> data_;
};