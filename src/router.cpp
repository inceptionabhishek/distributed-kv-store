#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include "simple_hash.hpp"

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

// The router holds one stub per storage node, and picks which stub
// to use for a given key via hash(key) % N. This is deliberately the
// naive scheme -- we'll replace this exact function body with
// consistent hashing in Stage 4, and nothing else about the router
// will need to change. Notice that fact once we get there.
class Router {
public:
    explicit Router(const std::vector<std::string>& node_addresses) {
        for (const auto& addr : node_addresses) {
            auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            stubs_.push_back(KVStoreService::NewStub(channel));
        }
    }

    size_t NodeIndexFor(const std::string& key) const {
        return fnv1a_hash(key) % stubs_.size();
    }

    bool Put(const std::string& key, const std::string& value) {
        size_t idx = NodeIndexFor(key);
        PutRequest request;
        request.set_key(key);
        request.set_value(value);
        PutResponse response;
        ClientContext context;

        Status status = stubs_[idx]->Put(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Put failed for key '" << key << "': "
                      << status.error_message() << std::endl;
            return false;
        }
        std::cout << "PUT   " << key << " -> node " << idx << std::endl;
        return response.success();
    }

    bool Get(const std::string& key, std::string* out_value) {
        size_t idx = NodeIndexFor(key);
        GetRequest request;
        request.set_key(key);
        GetResponse response;
        ClientContext context;

        Status status = stubs_[idx]->Get(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Get failed for key '" << key << "': "
                      << status.error_message() << std::endl;
            return false;
        }
        std::cout << "GET   " << key << " -> node " << idx
                   << " (found=" << response.found() << ")" << std::endl;
        if (response.found()) {
            *out_value = response.value();
        }
        return response.found();
    }

private:
    std::vector<std::unique_ptr<KVStoreService::Stub>> stubs_;
};

int main() {
    // Three storage nodes, expected to already be running.
    std::vector<std::string> nodes = {
        "localhost:50051",
        "localhost:50052",
        "localhost:50053",
        "localhost:50054",
    };

    Router router(nodes);

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