#include "kv_store.hpp"

void KVStore::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);   // released automatically when lock goes out of scope
    data_[key] = value;
}

std::optional<std::string> KVStore::get(const std::string& key) const {
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