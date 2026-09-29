#include "node_service.hpp"
#include <thread>
namespace {
grpc::Status IOFailure(const std::exception& e) { return {grpc::StatusCode::INTERNAL, e.what()}; }
}
grpc::Status NodeService::Fault(grpc::ServerContext* context, bool data) {
    if (unavailable) return {grpc::StatusCode::UNAVAILABLE, "injected unavailability"};
    unsigned every = fail_every.load();
    if (data && every && ++calls_ % every == 0) return {grpc::StatusCode::UNAVAILABLE, "injected dropped request"};
    if (data) {
        auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms.load());
        while (std::chrono::steady_clock::now() < end) {
            if (context->IsCancelled()) return {grpc::StatusCode::CANCELLED, "request cancelled"};
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }
    if (context->IsCancelled()) return {grpc::StatusCode::CANCELLED, "request cancelled"};
    return grpc::Status::OK;
}
grpc::Status NodeService::Put(grpc::ServerContext* c, const kvstore::PutRequest* request, kvstore::PutResponse* response) {
    auto start = std::chrono::steady_clock::now(); ++metrics_.writes;
    auto fault = Fault(c, true); if (!fault.ok()) { ++metrics_.rpc_failures; return fault; }
    if (request->key().size() > 4096 || request->value().size() > 1024 * 1024)
        return {grpc::StatusCode::INVALID_ARGUMENT, "key/value too large"};
    auto entry = FromWire(*request);
    if (entry.data.version.writer.empty() || (entry.data.tombstone && !entry.data.value.empty()))
        return {grpc::StatusCode::INVALID_ARGUMENT, "invalid version/tombstone"};
    try {
        auto result = store_.apply(entry); clock_.Observe(entry.data.version);
        response->set_success(result == ApplyResult::Applied || result == ApplyResult::Duplicate);
        response->set_result(result == ApplyResult::Applied ? kvstore::APPLIED :
            result == ApplyResult::Duplicate ? kvstore::DUPLICATE :
            result == ApplyResult::Stale ? kvstore::STALE : kvstore::CONFLICT);
        if (result == ApplyResult::Conflict) ++metrics_.conflicts;
        auto current = store_.raw_get(entry.key);
        if (current) ToWire(current->version, response->mutable_current_version());
        metrics_.Observe(start); return grpc::Status::OK;
    } catch (const std::exception& e) { return IOFailure(e); }
}
grpc::Status NodeService::Get(grpc::ServerContext* c, const kvstore::GetRequest* request, kvstore::GetResponse* response) {
    auto start = std::chrono::steady_clock::now(); ++metrics_.reads;
    auto fault = Fault(c, true); if (!fault.ok()) return fault;
    auto result = store_.raw_get(request->key()); response->set_quorum_ok(true);
    if (result) ToWire(*result, response);
    metrics_.Observe(start); return grpc::Status::OK;
}
grpc::Status NodeService::Remove(grpc::ServerContext* c, const kvstore::RemoveRequest* request, kvstore::RemoveResponse* response) {
    auto fault = Fault(c, true); if (!fault.ok()) return fault;
    try {
        auto prior = store_.raw_get(request->key());
        if (prior) clock_.Observe(prior->version);
        auto v = request->has_version() ? FromWire(request->version()) : clock_.Next();
        auto result = store_.apply({request->key(), {"", v.physical_ms, v, true}});
        response->set_removed(result == ApplyResult::Applied || result == ApplyResult::Duplicate);
        ToWire(v, response->mutable_version()); return grpc::Status::OK;
    } catch (const std::exception& e) { return IOFailure(e); }
}
grpc::Status NodeService::Ping(grpc::ServerContext* c, const kvstore::PingRequest*, kvstore::PingResponse* response) {
    auto fault = Fault(c, false); if (!fault.ok()) return fault;
    response->set_alive(true); return grpc::Status::OK;
}
grpc::Status NodeService::Scan(grpc::ServerContext* c, const kvstore::ScanRequest*, grpc::ServerWriter<kvstore::Record>* writer) {
    auto fault = Fault(c, true); if (!fault.ok()) return fault;
    auto entries = store_.scan();
    if (entries.size() > 100000) return {grpc::StatusCode::RESOURCE_EXHAUSTED, "demo scan limit exceeded"};
    for (const auto& entry : entries) {
        kvstore::Record record; ToWire(entry, &record);
        if (c->IsCancelled() || !writer->Write(record)) return {grpc::StatusCode::CANCELLED, "scan interrupted"};
    }
    return grpc::Status::OK;
}
grpc::Status NodeService::Snapshot(grpc::ServerContext*, const kvstore::SnapshotRequest*, kvstore::AdminResponse* response) {
    try { store_.snapshot(); response->set_success(true); return grpc::Status::OK; }
    catch (const std::exception& e) { return IOFailure(e); }
}
std::string NodeService::MetricsText() const {
    std::string mode = store_.durability() == Durability::Always ? "always" :
        store_.durability() == Durability::Periodic ? "periodic" : "memory";
    return metrics_.Text() + "kv_keys_stored " + std::to_string(store_.size()) +
        "\nkv_durability_info{mode=\"" + mode + "\"} 1" +
        "\nkv_records_stored " + std::to_string(store_.scan().size()) +
        "\nkv_periodic_fsync_total " + std::to_string(store_.sync_count()) +
        "\nkv_wal_fsync_seconds_count " + std::to_string(store_.wal_sync_count()) +
        "\nkv_wal_fsync_seconds_sum " + std::to_string(store_.wal_sync_us() / 1e6) + "\n";
}
grpc::Status NodeService::Metrics(grpc::ServerContext*, const kvstore::MetricsRequest*, kvstore::MetricsResponse* response) {
    response->set_text(MetricsText()); return grpc::Status::OK;
}
