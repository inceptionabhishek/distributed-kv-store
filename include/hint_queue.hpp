#pragma once
#include "kv_store.hpp"
#include <map>

struct Hint {
    std::string node;
    Entry entry;
    uint64_t created_ms = 0, next_retry_ms = 0;
    unsigned attempts = 0;
};
class HintQueue {
public:
    explicit HintQueue(const std::string& directory = "", uint64_t ttl_ms = 86400000);
    void Store(const std::string& node, const Entry& entry);
    std::vector<Hint> Due() const;
    std::vector<Hint> All() const;
    void Complete(const Hint& hint);
    void Failed(const Hint& hint);
    void DropTargetsExcept(const std::vector<std::string>& nodes);
    size_t size() const;
    uint64_t expired() const;
    void Snapshot();
private:
    using Key = std::pair<std::string, std::string>;
    void Persist(bool erase, const Hint& h);
    void CompactLocked();
    mutable std::mutex mutex_;
    std::map<Key, Hint> hints_;
    std::unique_ptr<Journal> journal_;
    uint64_t ttl_ms_, expired_ = 0;
    size_t changes_ = 0;
};
