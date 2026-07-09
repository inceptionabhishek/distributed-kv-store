#include <iostream>
#include <memory>
#include <string>
#include <cstdlib>

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

void RunServer(const std::string& server_address) {
    KVStoreServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "KV server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    // Usage: ./kv_server [port]   (defaults to 50051 if not given)
    int port = 50051;
    if (argc > 1) {
        port = std::atoi(argv[1]);
    }
    std::string server_address = "0.0.0.0:" + std::to_string(port);
    RunServer(server_address);
    return 0;
}