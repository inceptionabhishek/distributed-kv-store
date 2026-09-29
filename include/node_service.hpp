#pragma once
#include "kvstore.grpc.pb.h"
#include "metrics.hpp"
#include "wire.hpp"

// Fault controls are local constructor options; no unauthenticated remote fault API.
class NodeService final : public kvstore::KVStoreService::Service {
public:
    NodeService(std::string directory = "", Durability durability = Durability::Memory,
                size_t snapshot_every = 10000)
        : store_(std::move(directory), durability, snapshot_every), clock_(UniqueWriter("storage")) {
        for (const auto& entry : store_.scan()) clock_.Observe(entry.data.version);
    }
    std::atomic<int> delay_ms{0};
    std::atomic<bool> unavailable{false};
    std::atomic<unsigned> fail_every{0};
    KVStore& store() { return store_; }
    std::string MetricsText() const;
    grpc::Status Put(grpc::ServerContext*, const kvstore::PutRequest*, kvstore::PutResponse*) override;
    grpc::Status Get(grpc::ServerContext*, const kvstore::GetRequest*, kvstore::GetResponse*) override;
    grpc::Status Remove(grpc::ServerContext*, const kvstore::RemoveRequest*, kvstore::RemoveResponse*) override;
    grpc::Status Ping(grpc::ServerContext*, const kvstore::PingRequest*, kvstore::PingResponse*) override;
    grpc::Status Scan(grpc::ServerContext*, const kvstore::ScanRequest*, grpc::ServerWriter<kvstore::Record>*) override;
    grpc::Status Snapshot(grpc::ServerContext*, const kvstore::SnapshotRequest*, kvstore::AdminResponse*) override;
    grpc::Status Metrics(grpc::ServerContext*, const kvstore::MetricsRequest*, kvstore::MetricsResponse*) override;
    grpc::Status Reserve(grpc::ServerContext*, const kvstore::PingRequest*, kvstore::Version* out) override {
        ToWire(clock_.Next(), out); return grpc::Status::OK;
    }
private:
    grpc::Status Fault(grpc::ServerContext* context, bool data);
    KVStore store_;
    HybridClock clock_;
    ::Metrics metrics_;
    std::atomic<uint64_t> calls_{0};
};
