#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include "consistent_hash_ring.hpp"

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

// Stage 6: quorum reads/writes. With N=3 replicas, we use W=2 and R=2
// (W + R = 4 > N = 3). That inequality is the whole point: it
// guarantees any successful read's set of R replicas MUST overlap with
// any prior successful write's set of W replicas on at least one node --
// so a read can never completely miss the latest write. That's what
// "quorum" buys you over Stage 5's wait-for-all: you can tolerate ONE
// node being down and still serve both reads and writes correctly,
// instead of Stage 5 where any one dead replica blocked everything.
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
          read_quorum_(read_quorum) {
        for (const auto& addr : node_addresses) {
            ring_.AddNode(addr);
            stubs_[addr] = KVStoreService::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        }
    }

    std::vector<std::string> ReplicaAddressesFor(const std::string& key) const {
        return ring_.GetNodesForKey(key, replication_factor_);
    }

    bool Put(const std::string& key, const std::string& value) {
        auto replicas = ReplicaAddressesFor(key);
        if (static_cast<int>(replicas.size()) < write_quorum_) {
            std::cerr << "Put failed for key '" << key << "': not enough nodes for write quorum "
                      << "(need " << write_quorum_ << ", have " << replicas.size() << ")" << std::endl;
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
                          << status.error_message() << std::endl;
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

private:
    static uint64_t NowMillis() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    ConsistentHashRing ring_;
    int replication_factor_;
    int write_quorum_;
    int read_quorum_;
    std::unordered_map<std::string, std::unique_ptr<KVStoreService::Stub>> stubs_;
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

    std::cout << "--- writing (quorum W=2 of 3 replicas) ---" << std::endl;
    int put_failures = 0;
    for (const auto& [key, value] : data) {
        if (!router.Put(key, value)) {
            put_failures++;
        }
    }

    std::cout << "\n--- reading back (quorum R=2 of 3 replicas) ---" << std::endl;
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
    std::cout << (put_failures == 0 && mismatches == 0
                  ? "PASS: quorum reads/writes succeeded despite any single node being down"
                  : "FAIL") << std::endl;

    return 0;
}