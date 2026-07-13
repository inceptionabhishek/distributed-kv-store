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

// The router now delegates "which node owns this key" entirely to the
// ConsistentHashRing. Compare this class to the Stage 3 version --
// every method's *body* is unchanged except NodeAddressFor(). That's
// the point: consistent hashing is a drop-in replacement for the
// routing decision, not a redesign of the router itself.
class Router {
public:
    explicit Router(const std::vector<std::string>& node_addresses, int virtual_nodes = 10)
        : ring_(virtual_nodes) {
        for (const auto& addr : node_addresses) {
            ring_.AddNode(addr);
            stubs_[addr] = KVStoreService::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        }
    }

    std::string NodeAddressFor(const std::string& key) const {
        return ring_.GetNodeForKey(key);
    }

    bool Put(const std::string& key, const std::string& value) {
        std::string addr = NodeAddressFor(key);
        PutRequest request;
        request.set_key(key);
        request.set_value(value);
        PutResponse response;
        ClientContext context;

        Status status = stubs_[addr]->Put(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Put failed for key '" << key << "': "
                      << status.error_message() << std::endl;
            return false;
        }
        std::cout << "PUT   " << key << " -> " << addr << std::endl;
        return response.success();
    }

    bool Get(const std::string& key, std::string* out_value) {
        std::string addr = NodeAddressFor(key);
        GetRequest request;
        request.set_key(key);
        GetResponse response;
        ClientContext context;

        Status status = stubs_[addr]->Get(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Get failed for key '" << key << "': "
                      << status.error_message() << std::endl;
            return false;
        }
        std::cout << "GET   " << key << " -> " << addr
                   << " (found=" << response.found() << ")" << std::endl;
        if (response.found()) {
            *out_value = response.value();
        }
        return response.found();
    }

private:
    ConsistentHashRing ring_;
    std::unordered_map<std::string, std::unique_ptr<KVStoreService::Stub>> stubs_;
};

int main() {
    std::vector<std::string> nodes = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        "localhost:50054",
    };

    Router router(nodes, 10);

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
    for (const auto& [key, value] : data) {
        router.Put(key, value);
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

    std::cout << "\n" << (mismatches == 0 ? "PASS: all keys routed and retrieved correctly"
                                            : "FAIL: some keys were wrong") << std::endl;

    return 0;
}