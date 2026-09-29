#include "kv_store.hpp"
#include <stdexcept>
std::string EncodeEntry(const Entry& e) {
    std::string out;
    binary::String(out, e.key); binary::String(out, e.data.value);
    binary::U64(out, e.data.version.physical_ms); binary::U64(out, e.data.version.logical);
    binary::String(out, e.data.version.writer); binary::U64(out, e.data.tombstone ? 1 : 0);
    return out;
}
Entry DecodeEntry(const std::string& bytes) {
    size_t p = 0; Entry e;
    e.key = binary::String(bytes, p); e.data.value = binary::String(bytes, p);
    e.data.version.physical_ms = binary::U64(bytes, p); e.data.timestamp = e.data.version.physical_ms;
    e.data.version.logical = binary::U64(bytes, p); e.data.version.writer = binary::String(bytes, p);
    auto tombstone = binary::U64(bytes, p);
    if (tombstone > 1 || p != bytes.size()) throw std::runtime_error("invalid entry record");
    e.data.tombstone = tombstone != 0; return e;
}
bool SamePayload(const TimestampedValue& a, const TimestampedValue& b) {
    return a.value == b.value && a.tombstone == b.tombstone;
}
KVStore::KVStore(std::string directory, Durability durability, size_t snapshot_every)
    : durability_(durability), snapshot_every_(snapshot_every) {
    if (durability != Durability::Memory) {
        if (directory.empty()) throw std::invalid_argument("persistent store needs a data directory");
        journal_ = std::make_unique<Journal>(directory, "store");
        journal_->Recover([this](const std::string& bytes) { Load(DecodeEntry(bytes)); });
    }
    if (durability == Durability::Periodic) sync_worker_ = std::thread([this] {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!wake_.wait_for(lock, std::chrono::milliseconds(100), [this] { return stop_; })) {
            try { journal_->Sync(); ++sync_count_; } catch (...) { /* subsequent writes fail closed */ }
        }
    });
}
KVStore::~KVStore() {
    { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
    wake_.notify_all(); if (sync_worker_.joinable()) sync_worker_.join();
    if (journal_) { try { journal_->Sync(); } catch (...) {} }
}
void KVStore::Load(const Entry& e) {
    auto it = data_.find(e.key);
    if (it == data_.end() || it->second.version < e.data.version) data_[e.key] = e.data;
    else if (it->second.version == e.data.version && !SamePayload(it->second, e.data))
        throw std::runtime_error("conflicting versions in journal");
}
ApplyResult KVStore::apply(const Entry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(entry.key);
    if (it != data_.end()) {
        if (entry.data.version < it->second.version) return ApplyResult::Stale;
        if (entry.data.version == it->second.version)
            return SamePayload(entry.data, it->second) ? ApplyResult::Duplicate : ApplyResult::Conflict;
    }
    if (journal_) journal_->Append(EncodeEntry(entry), durability_ == Durability::Always);
    auto data = entry.data; data.timestamp = data.version.physical_ms;
    data_[entry.key] = std::move(data);
    if (journal_ && snapshot_every_ && ++mutations_ >= snapshot_every_) SnapshotLocked();
    return ApplyResult::Applied;
}
ApplyResult KVStore::put(const std::string& key, const std::string& value, uint64_t timestamp) {
    return apply({key, {value, timestamp, {timestamp, 0, "legacy"}, false}});
}
std::optional<TimestampedValue> KVStore::raw_get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key); if (it == data_.end()) return std::nullopt; return it->second;
}
std::optional<TimestampedValue> KVStore::get(const std::string& key) const {
    auto result = raw_get(key); if (result && result->tombstone) return std::nullopt; return result;
}
bool KVStore::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it == data_.end() || it->second.tombstone) return false;
    auto deleted = it->second; deleted.value.clear(); deleted.tombstone = true; ++deleted.version.logical;
    Entry e{key, deleted};
    if (journal_) journal_->Append(EncodeEntry(e), durability_ == Durability::Always);
    it->second = deleted; return true;
}
size_t KVStore::size() const {
    std::lock_guard<std::mutex> lock(mutex_); size_t size = 0;
    for (const auto& item : data_) if (!item.second.tombstone) ++size;
    return size;
}
std::vector<Entry> KVStore::scan() const {
    std::lock_guard<std::mutex> lock(mutex_); std::vector<Entry> entries;
    entries.reserve(data_.size()); for (const auto& item : data_) entries.push_back({item.first, item.second});
    return entries;
}
void KVStore::SnapshotLocked() {
    std::vector<std::string> records; records.reserve(data_.size());
    for (const auto& item : data_) records.push_back(EncodeEntry({item.first, item.second}));
    journal_->Checkpoint(records); mutations_ = 0;
}
void KVStore::snapshot() { std::lock_guard<std::mutex> lock(mutex_); if (journal_) SnapshotLocked(); }
