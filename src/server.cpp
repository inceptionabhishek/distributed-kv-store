#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include "kv_store.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using kvstore::KVStoreService;
using kvstore::PutRequest;
using kvstore::PutResponse;
using kvstore::GetRequest;
using kvstore::GetResponse;
using kvstore::RemoveRequest;
using kvstore::RemoveResponse;

// This class is the network-facing adapter. Notice it doesn't reimplement
// any storage logic -- it just translates gRPC calls into calls on the
// KVStore we already built and tested in Stage 1. That separation matters:
// KVStore has zero knowledge that gRPC exists, so we can unit-test it
// without ever spinning up a server or a socket.
class KVStoreServiceImpl final : public KVStoreService::Service {
public:
    Status Put(ServerContext* context, const PutRequest* request,
               PutResponse* response) override {
        store_.put(request->key(), request->value());
        response->set_success(true);
        return Status::OK;
    }

    Status Get(ServerContext* context, const GetRequest* request,
               GetResponse* response) override {
        auto value = store_.get(request->key());
        if (value.has_value()) {
            response->set_found(true);
            response->set_value(*value);
        } else {
            response->set_found(false);
        }
        return Status::OK;
    }

    Status Remove(ServerContext* context, const RemoveRequest* request,
                  RemoveResponse* response) override {
        bool removed = store_.remove(request->key());
        response->set_removed(removed);
        return Status::OK;
    }

private:
    KVStore store_;
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    KVStoreServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "KV server listening on " << server_address << std::endl;
    server->Wait();   // blocks here, handling requests, until the process is killed
}

int main() {
    RunServer();
    return 0;
}