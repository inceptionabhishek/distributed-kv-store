#pragma once
#include "journal.hpp"
#include "version.hpp"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <optional>
#include <thread>
#include <unordered_map>
enum class Durability { Memory, Always, Periodic };
enum class ApplyResult { Applied, Duplicate, Stale, Conflict };
struct TimestampedValue {
    std::string value;
    uint64_t timestamp = 0;
    Version version;
    bool tombstone = false;
};
struct Entry { std::string key; TimestampedValue data; };
std::string EncodeEntry(const Entry& entry);
Entry DecodeEntry(const std::string& bytes);
bool SamePayload(const TimestampedValue& a, const TimestampedValue& b);
class KVStore {
public:
    explicit KVStore(std::string directory = "", Durability durability = Durability::Memory,
                     size_t snapshot_every = 10000);
    ~KVStore();
    ApplyResult apply(const Entry& entry);
    ApplyResult put(const std::string& key, const std::string& value, uint64_t timestamp);
    std::optional<TimestampedValue> get(const std::string& key) const;
    std::optional<TimestampedValue> raw_get(const std::string& key) const;
    bool remove(const std::string& key);
    size_t size() const;
    std::vector<Entry> scan() const;
    void snapshot();
    uint64_t sync_count() const { return sync_count_.load(); }
    Durability durability() const { return durability_; }
    uint64_t wal_sync_count() const { return journal_ ? journal_->SyncCount() : 0; }
    uint64_t wal_sync_us() const { return journal_ ? journal_->SyncMicroseconds() : 0; }
private:
    void SnapshotLocked();
    void Load(const Entry& entry);
    mutable std::mutex mutex_;
    std::condition_variable wake_;
    std::unordered_map<std::string, TimestampedValue> data_;
    std::unique_ptr<Journal> journal_;
    Durability durability_;
    size_t snapshot_every_, mutations_ = 0;
    bool stop_ = false;
    std::thread sync_worker_;
    std::atomic<uint64_t> sync_count_{0};
};
