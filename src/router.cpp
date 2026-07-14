#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include "consistent_hash_ring.hpp"
#include "failure_detector.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using kvstore::KVStoreService;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::RemoveRequest;
using kvstore::RemoveResponse;

// A write meant for a node that was down at the time. Kept in memory
// until that node comes back, then replayed and discarded. This is a
// simplified, router-side version of "hinted handoff" -- real systems
// (e.g. Cassandra) store hints on ANOTHER live replica rather than on
// the coordinator itself, so the hint survives even if the coordinator
// process dies. Our version doesn't survive a router crash; that's a
// known, deliberate simplification for teaching purposes.
struct Hint {
    std::string key;
    std::string value;
    uint64_t timestamp;
    std::string intended_node;
};

class Router {
public:
    explicit Router(const std::vector<std::string>& node_addresses,
                     int virtual_nodes = 10,
                     int replication_factor = 3,
                     int write_quorum = 2,
                     int read_quorum = 2)
        : ring_(virtual_nodes),
          replication_factor_(replication_factor),
          write_quorum_(write_quorum),
          read_quorum_(read_quorum),
          failure_detector_(node_addresses) {
        for (const auto& addr : node_addresses) {
            ring_.AddNode(addr);
            stubs_[addr] = KVStoreService::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        }
        failure_detector_.Start();
    }

    ~Router() {
        failure_detector_.Stop();
    }

    std::vector<std::string> ReplicaAddressesFor(const std::string& key) const {
        return ring_.GetNodesForKey(key, replication_factor_);
    }

    bool Put(const std::string& key, const std::string& value) {
        auto replicas = ReplicaAddressesFor(key);
        if (static_cast<int>(replicas.size()) < write_quorum_) {
            std::cerr << "Put failed for key '" << key << "': not enough nodes for write quorum"
                      << std::endl;
            return false;
        }

        uint64_t timestamp = NowMillis();

        std::cout << "PUT   " << key << " (ts=" << timestamp << ") -> replicas: [";
        for (size_t i = 0; i < replicas.size(); ++i) {
            std::cout << replicas[i] << (i + 1 < replicas.size() ? ", " : "");
        }
        std::cout << "]" << std::endl;

        int successes = 0;
        for (const auto& addr : replicas) {
            if (!failure_detector_.IsAlive(addr)) {
                std::cout << "  " << addr << " known DOWN, skipping RPC, storing hint" << std::endl;
                StoreHint(addr, {key, value, timestamp, addr});
                continue;
            }

            PutRequest request;
            request.set_key(key);
            request.set_value(value);
            request.set_timestamp(timestamp);
            PutResponse response;
            ClientContext context;

            Status status = stubs_[addr]->Put(&context, request, &response);
            if (status.ok() && response.success()) {
                successes++;
            } else {
                std::cerr << "  Put to replica " << addr << " FAILED: "
                          << status.error_message() << " -- storing hint" << std::endl;
                StoreHint(addr, {key, value, timestamp, addr});
            }
        }

        bool ok = successes >= write_quorum_;
        std::cout << "  " << successes << "/" << replicas.size() << " replicas acked "
                   << "(needed " << write_quorum_ << ") -> "
                   << (ok ? "SUCCESS" : "FAILED") << std::endl;
        return ok;
    }

    bool Get(const std::string& key, std::string* out_value) {
        auto replicas = ReplicaAddressesFor(key);
        if (static_cast<int>(replicas.size()) < read_quorum_) {
            std::cerr << "Get failed for key '" << key << "': not enough nodes for read quorum"
                      << std::endl;
            return false;
        }

        struct Reply { bool found; std::string value; uint64_t timestamp; };
        std::vector<Reply> replies;

        for (const auto& addr : replicas) {
            if (!failure_detector_.IsAlive(addr)) {
                std::cout << "  " << addr << " known DOWN, skipping" << std::endl;
                continue;
            }

            GetRequest request;
            request.set_key(key);
            GetResponse response;
            ClientContext context;

            Status status = stubs_[addr]->Get(&context, request, &response);
            if (!status.ok()) {
                std::cerr << "  Get from replica " << addr << " FAILED: "
                          << status.error_message() << std::endl;
                continue;
            }
            replies.push_back({response.found(), response.value(), response.timestamp()});
            if (static_cast<int>(replies.size()) >= read_quorum_) {
                break;
            }
        }

        if (static_cast<int>(replies.size()) < read_quorum_) {
            std::cerr << "Get failed for key '" << key << "': only got "
                      << replies.size() << "/" << read_quorum_ << " needed replies" << std::endl;
            return false;
        }

        const Reply* best = nullptr;
        for (const auto& r : replies) {
            if (!r.found) continue;
            if (best == nullptr || r.timestamp > best->timestamp) {
                best = &r;
            }
        }

        std::cout << "GET   " << key << " -> queried " << replies.size()
                   << " replicas (needed " << read_quorum_ << ")" << std::endl;

        if (best == nullptr) {
            return false;
        }
        *out_value = best->value;
        return true;
    }

    // Call this periodically to replay any hints whose destination node
    // has come back up.
    void ReplayHintsForRecoveredNodes() {
        std::lock_guard<std::mutex> lock(hints_mutex_);
        for (auto it = hints_.begin(); it != hints_.end(); ) {
            const std::string& node = it->first;
            if (failure_detector_.IsAlive(node)) {
                std::vector<Hint>& pending = it->second;
                std::cout << "[hinted-handoff] " << node << " is back UP, replaying "
                          << pending.size() << " hint(s)" << std::endl;
                for (const auto& hint : pending) {
                    PutRequest request;
                    request.set_key(hint.key);
                    request.set_value(hint.value);
                    request.set_timestamp(hint.timestamp);
                    PutResponse response;
                    ClientContext context;
                    Status status = stubs_[node]->Put(&context, request, &response);
                    std::cout << "  replayed key='" << hint.key << "' to " << node
                              << " -> " << (status.ok() ? "ok" : "FAILED") << std::endl;
                }
                it = hints_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    static uint64_t NowMillis() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    void StoreHint(const std::string& dead_node, Hint hint) {
        std::lock_guard<std::mutex> lock(hints_mutex_);
        hints_[dead_node].push_back(std::move(hint));
    }

    ConsistentHashRing ring_;
    int replication_factor_;
    int write_quorum_;
    int read_quorum_;
    FailureDetector failure_detector_;
    std::unordered_map<std::string, std::unique_ptr<KVStoreService::Stub>> stubs_;

    std::mutex hints_mutex_;
    std::unordered_map<std::string, std::vector<Hint>> hints_;
};

int main() {
    std::vector<std::string> nodes = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        "localhost:50054"
    };

    Router router(nodes, /*virtual_nodes=*/10, /*replication_factor=*/3,
                  /*write_quorum=*/2, /*read_quorum=*/2);

    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    std::vector<std::pair<std::string, std::string>> data = {
        {"user:1", "abhishek"},
        {"user:2", "priya"},
        {"user:3", "rahul"},
        {"user:4", "meera"},
        {"user:5", "arjun"},
        {"order:100", "shipped"},
        {"order:101", "pending"},
        {"order:102", "delivered"},
    };

    std::cout << "--- writing ---" << std::endl;
    int put_failures = 0;
    for (const auto& [key, value] : data) {
        if (!router.Put(key, value)) {
            put_failures++;
        }
        router.ReplayHintsForRecoveredNodes();
    }

    std::cout << "\n--- reading back ---" << std::endl;
    int mismatches = 0;
    for (const auto& [key, expected_value] : data) {
        std::string actual_value;
        bool found = router.Get(key, &actual_value);
        if (!found || actual_value != expected_value) {
            std::cout << "MISMATCH on key " << key << std::endl;
            mismatches++;
        }
    }

    std::cout << "\nput_failures=" << put_failures << " mismatches=" << mismatches << std::endl;
    std::cout << (put_failures == 0 && mismatches == 0 ? "PASS" : "FAIL") << std::endl;

    return 0;
}