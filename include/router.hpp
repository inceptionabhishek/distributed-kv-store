#pragma once
#include "consistent_hash_ring.hpp"
#include "failure_detector.hpp"
#include "hint_queue.hpp"
#include "metrics.hpp"
#include "thread_pool.hpp"
#include "wire.hpp"
#include <condition_variable>
#include <map>
#include <shared_mutex>

struct RouterOptions {
    int virtual_nodes = 100, replication_factor = 3, write_quorum = 2, read_quorum = 2;
    size_t workers = 24, queue_capacity = 4096;
    std::chrono::milliseconds operation_timeout{750}, replica_timeout{500}, repair_interval{5000};
    std::string state_directory;
    uint64_t hint_ttl_ms = 86400000;
    bool verbose = false, sequential = false; // sequential mode is a benchmark baseline
};
enum class ReadStatus { Found, NotFound, Unavailable, Conflict };
struct ReadResult {
    ReadStatus status = ReadStatus::Unavailable;
    std::optional<TimestampedValue> data;
};
struct WriteResult {
    bool success = false;
    kvstore::WriteStatus status = kvstore::QUORUM_FAILED;
    Version version;
};
class Router {
public:
    Router(const std::vector<std::string>& nodes, RouterOptions options);
    explicit Router(const std::vector<std::string>& nodes, int virtual_nodes = 100,
                    int replication_factor = 3, int write_quorum = 2, int read_quorum = 2,
                    bool verbose = true);
    ~Router();
    Router(const Router&) = delete;
    Router& operator=(const Router&) = delete;
    Version NewVersion();
    Entry NewEntry(const std::string& key, const std::string& value, bool tombstone = false);
    WriteResult Mutate(const Entry& entry,
        std::chrono::system_clock::time_point parent_deadline = std::chrono::system_clock::time_point::max());
    bool Put(const std::string& key, const std::string& value);
    bool Remove(const std::string& key);
    ReadResult Read(const std::string& key,
        std::chrono::system_clock::time_point parent_deadline = std::chrono::system_clock::time_point::max());
    bool Get(const std::string& key, std::string* out);
    std::vector<std::string> ReplicaAddressesFor(const std::string& key) const;
    void ReplayHintsForRecoveredNodes();
    bool RepairOnce();
    bool Reconfigure(const std::vector<std::string>& nodes, uint64_t epoch, std::string* error);
    uint64_t Epoch() const;
    size_t PendingHints() const { return hints_.size(); }
    std::string MetricsText() const;
    void SnapshotHints() { hints_.Snapshot(); }
    void Drain() { pool_.Drain(); }
private:
    using Stub = kvstore::KVStoreService::Stub;
    struct Topology {
        explicit Topology(int virtual_nodes) : ring(virtual_nodes) {}
        ConsistentHashRing ring;
        std::vector<std::string> nodes;
        std::map<std::string, std::shared_ptr<Stub>> stubs;
        std::shared_ptr<FailureDetector> detector;
        uint64_t epoch = 1;
    };
    std::shared_ptr<Topology> BuildTopology(const std::vector<std::string>& nodes, uint64_t epoch);
    bool Scan(const std::shared_ptr<Topology>& topology, std::map<std::string, Entry>& union_data, bool strict);
    bool RepairLocked(const std::shared_ptr<Topology>& topology);
    void ReplayLocked(const std::shared_ptr<Topology>& topology);
    void Background();
    void SaveMembership(const Topology& topology);
    void RecordRPC(const grpc::Status& status);
    void Log(const char* operation, const std::string& key, bool success, const Version* version = nullptr) const;
    RouterOptions options_;
    mutable std::shared_mutex topology_mutex_;
    std::shared_ptr<Topology> topology_;
    HintQueue hints_;
    HybridClock clock_;
    std::mutex clock_mutex_;
    std::unique_ptr<Journal> clock_journal_, membership_journal_;
    size_t clock_records_ = 0;
    Metrics metrics_;
    ThreadPool pool_;
    std::atomic<bool> stop_{false};
    std::mutex background_mutex_, replay_mutex_;
    std::condition_variable background_wake_;
    std::thread background_;
};
