#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

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

class KVStoreClient {
public:
    KVStoreClient(std::shared_ptr<Channel> channel)
        : stub_(KVStoreService::NewStub(channel)) {}

    bool Put(const std::string& key, const std::string& value) {
        PutRequest request;
        request.set_key(key);
        request.set_value(value);

        PutResponse response;
        ClientContext context;

        Status status = stub_->Put(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Put RPC failed: " << status.error_message() << std::endl;
            return false;
        }
        return response.success();
    }

    bool Get(const std::string& key, std::string* out_value) {
        GetRequest request;
        request.set_key(key);

        GetResponse response;
        ClientContext context;

        Status status = stub_->Get(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Get RPC failed: " << status.error_message() << std::endl;
            return false;
        }
        if (response.found()) {
            *out_value = response.value();
        }
        return response.found();
    }

    bool Remove(const std::string& key) {
        RemoveRequest request;
        request.set_key(key);

        RemoveResponse response;
        ClientContext context;

        Status status = stub_->Remove(&context, request, &response);
        if (!status.ok()) {
            std::cerr << "Remove RPC failed: " << status.error_message() << std::endl;
            return false;
        }
        return response.removed();
    }

private:
    std::unique_ptr<KVStoreService::Stub> stub_;
};

int main() {
    KVStoreClient client(
        grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));

    client.Put("name", "abhishek");
    client.Put("role", "backend engineer");

    std::string value;
    if (client.Get("name", &value)) {
        std::cout << "name -> " << value << std::endl;
    }
    if (client.Get("role", &value)) {
        std::cout << "role -> " << value << std::endl;
    }

    bool removed = client.Remove("role");
    std::cout << "removed role? " << removed << std::endl;

    if (!client.Get("role", &value)) {
        std::cout << "role is a miss, as expected" << std::endl;
    }

    return 0;
}