#include "kv_store.hpp"

void KVStore::put(const std::string& key, const std::string& value) {
    data_[key] = value;   // inserts if new, overwrites if existing
}

std::optional<std::string> KVStore::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool KVStore::remove(const std::string& key) {
    return data_.erase(key) > 0;
}

size_t KVStore::size() const {
    return data_.size();
}