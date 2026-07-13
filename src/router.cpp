#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

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

// Stage 5: replication. Put now writes to REPLICATION_FACTOR nodes (the
// key's primary owner plus its ring successors) and only reports success
// if ALL of them acknowledge. Get, for now, still reads from just the
// primary -- we are not yet handling "what if replicas disagree", that's
// Stage 6's problem (quorum reads/writes + conflict resolution).
//
// Also notice the failure mode this creates on purpose: if ANY one of
// the 3 replicas is down, Put fails entirely, even though 2 of 3 copies
// are healthy. That's the real cost of "wait for all" replication --
// full consistency, poor availability. Quorum (Stage 6) is the fix.
class Router {
public:
    explicit Router(const std::vector<std::string>& node_addresses,
                     int virtual_nodes = 10,
                     int replication_factor = 3)
        : ring_(virtual_nodes), replication_factor_(replication_factor) {
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
        if (replicas.empty()) {
            std::cerr << "Put failed for key '" << key << "': no nodes in ring" << std::endl;
            return false;
        }

        std::cout << "PUT   " << key << " -> replicas: [";
        for (size_t i = 0; i < replicas.size(); ++i) {
            std::cout << replicas[i] << (i + 1 < replicas.size() ? ", " : "");
        }
        std::cout << "]" << std::endl;

        for (const auto& addr : replicas) {
            PutRequest request;
            request.set_key(key);
            request.set_value(value);
            PutResponse response;
            ClientContext context;

            Status status = stubs_[addr]->Put(&context, request, &response);
            if (!status.ok() || !response.success()) {
                std::cerr << "  Put to replica " << addr << " FAILED: "
                          << status.error_message() << std::endl;
                return false;
            }
        }
        return true;
    }

    bool Get(const std::string& key, std::string* out_value) {
        auto replicas = ReplicaAddressesFor(key);
        if (replicas.empty()) {
            return false;
        }
        const std::string& primary = replicas[0];

        GetRequest request;
        request.set_key(key);
        GetResponse response;
        ClientContext context;

        Status status = stubs_[primary]->Get(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Get failed for key '" << key << "' from primary " << primary
                      << ": " << status.error_message() << std::endl;
            return false;
        }
        std::cout << "GET   " << key << " -> primary " << primary
                   << " (found=" << response.found() << ")" << std::endl;
        if (response.found()) {
            *out_value = response.value();
        }
        return response.found();
    }

private:
    ConsistentHashRing ring_;
    int replication_factor_;
    std::unordered_map<std::string, std::unique_ptr<KVStoreService::Stub>> stubs_;
};

int main() {
    std::vector<std::string> nodes = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        "localhost:50054"
    };

    Router router(nodes, /*virtual_nodes=*/10, /*replication_factor=*/3);

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

    std::cout << "--- writing (replicated to 3 nodes each) ---" << std::endl;
    int put_failures = 0;
    for (const auto& [key, value] : data) {
        if (!router.Put(key, value)) {
            put_failures++;
        }
    }

    std::cout << "\n--- reading back (from primary only) ---" << std::endl;
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
                  ? "PASS: all keys replicated and retrieved correctly"
                  : "FAIL") << std::endl;

    return 0;
}