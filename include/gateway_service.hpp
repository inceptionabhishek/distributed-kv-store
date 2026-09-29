#pragma once
#include "router.hpp"
class GatewayService final : public kvstore::KVStoreService::Service {
public:
    explicit GatewayService(Router& router) : router_(router) {}
    grpc::Status Put(grpc::ServerContext* context, const kvstore::PutRequest* r, kvstore::PutResponse* out) override {
        try {
            auto e = r->has_version() ? FromWire(*r) : router_.NewEntry(r->key(), r->value(), r->tombstone());
            auto result = router_.Mutate(e, context->deadline());
            out->set_success(result.success); out->set_result(result.status);
            ToWire(result.version, out->mutable_current_version()); return grpc::Status::OK;
        } catch (const std::exception& e) { return {grpc::StatusCode::INVALID_ARGUMENT, e.what()}; }
    }
    grpc::Status Get(grpc::ServerContext* context, const kvstore::GetRequest* r, kvstore::GetResponse* out) override {
        auto result = router_.Read(r->key(), context->deadline());
        if (result.status == ReadStatus::Unavailable) return {grpc::StatusCode::UNAVAILABLE, "read quorum not reached"};
        if (result.status == ReadStatus::Conflict) return {grpc::StatusCode::DATA_LOSS, "equal-version conflict"};
        out->set_quorum_ok(true); if (result.data) ToWire(*result.data, out); return grpc::Status::OK;
    }
    grpc::Status Remove(grpc::ServerContext* context, const kvstore::RemoveRequest* r, kvstore::RemoveResponse* out) override {
        try {
            auto entry = router_.NewEntry(r->key(), "", true);
            if (r->has_version()) entry.data.version = FromWire(r->version());
            auto result = router_.Mutate(entry, context->deadline());
            out->set_removed(result.success); ToWire(result.version, out->mutable_version());
            if (!result.success) return {grpc::StatusCode::UNAVAILABLE, "delete quorum not reached"};
            return grpc::Status::OK;
        } catch (const std::exception& e) { return {grpc::StatusCode::INVALID_ARGUMENT, e.what()}; }
    }
    grpc::Status Ping(grpc::ServerContext*, const kvstore::PingRequest*, kvstore::PingResponse* out) override {
        out->set_alive(true); return grpc::Status::OK;
    }
    grpc::Status Debug(grpc::ServerContext*, const kvstore::DebugRequest* r, kvstore::DebugResponse* out) override {
        for (const auto& node : router_.ReplicaAddressesFor(r->key())) out->add_replicas(node);
        out->set_epoch(router_.Epoch()); return grpc::Status::OK;
    }
    grpc::Status Reconfigure(grpc::ServerContext*, const kvstore::ReconfigureRequest* r, kvstore::AdminResponse* out) override {
        std::vector<std::string> nodes(r->nodes().begin(), r->nodes().end()); std::string error;
        out->set_success(router_.Reconfigure(nodes, r->epoch(), &error));
        out->set_detail(out->success() ? "migration complete; epoch activated" : error); return grpc::Status::OK;
    }
    grpc::Status Repair(grpc::ServerContext*, const kvstore::PingRequest*, kvstore::AdminResponse* out) override {
        try { out->set_success(router_.RepairOnce()); out->set_detail("full scan repair attempted"); return grpc::Status::OK; }
        catch (const std::exception& e) { return {grpc::StatusCode::INTERNAL, e.what()}; }
    }
    grpc::Status Snapshot(grpc::ServerContext*, const kvstore::SnapshotRequest*, kvstore::AdminResponse* out) override {
        try { router_.SnapshotHints(); out->set_success(true); return grpc::Status::OK; }
        catch (const std::exception& e) { return {grpc::StatusCode::INTERNAL, e.what()}; }
    }
    grpc::Status Metrics(grpc::ServerContext*, const kvstore::MetricsRequest*, kvstore::MetricsResponse* out) override {
        out->set_text(router_.MetricsText()); return grpc::Status::OK;
    }
    grpc::Status Reserve(grpc::ServerContext*, const kvstore::PingRequest*, kvstore::Version* out) override {
        try { ToWire(router_.NewVersion(), out); return grpc::Status::OK; }
        catch (const std::exception& e) { return {grpc::StatusCode::INTERNAL, e.what()}; }
    }
private:
    Router& router_;
};
