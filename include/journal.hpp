#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <atomic>

// Caller serializes access. A directory lock prevents concurrent process ownership.
// A failed append/fsync poisons the journal until recovery.
class Journal {
public:
    Journal(const std::string& directory, const std::string& name);
    ~Journal();
    Journal(const Journal&) = delete;
    Journal& operator=(const Journal&) = delete;
    void Recover(const std::function<void(const std::string&)>& consume);
    void Append(const std::string& payload, bool sync);
    void Sync();
    void Checkpoint(const std::vector<std::string>& records);
    uint64_t SyncCount() const { return sync_count_.load(); }
    uint64_t SyncMicroseconds() const { return sync_us_.load(); }
private:
    std::string directory_, wal_, snapshot_;
    int fd_ = -1, lock_fd_ = -1;
    bool poisoned_ = false;
    std::atomic<uint64_t> sync_count_{0}, sync_us_{0};
};
namespace binary {
void U64(std::string& out, uint64_t n);
uint64_t U64(const std::string& in, size_t& offset);
void String(std::string& out, const std::string& s);
std::string String(const std::string& in, size_t& offset);
}
