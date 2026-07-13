#include "kv_store.hpp"

void KVStore::put(const std::string& key, const std::string& value, uint64_t timestamp) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end() || timestamp >= it->second.timestamp) {
        data_[key] = TimestampedValue{value, timestamp};
    }
    // else: an older write arrived late -- silently ignored, last-write-wins.
}

std::optional<TimestampedValue> KVStore::get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool KVStore::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.erase(key) > 0;
}

size_t KVStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.size();
}