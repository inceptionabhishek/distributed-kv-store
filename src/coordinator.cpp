#include "router.hpp"
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <cctype>

namespace {
std::string Identity() {
    std::random_device rng;
    return "coordinator-" + std::to_string(WallMillis()) + "-" + std::to_string(rng()) + "-" + std::to_string(rng());
}
RouterOptions LegacyOptions(int v, int n, int w, int r, bool verbose) {
    RouterOptions o; o.virtual_nodes = v; o.replication_factor = n;
    o.write_quorum = w; o.read_quorum = r; o.verbose = verbose; return o;
}
using Deadline = std::chrono::system_clock::time_point;
Deadline RPCDeadline(const RouterOptions& options, Deadline operation) {
    return std::min(operation, std::chrono::system_clock::now() + options.replica_timeout);
}
void Merge(std::map<std::string, Entry>& data, const Entry& e) {
    auto it = data.find(e.key);
    if (it == data.end() || it->second.data.version < e.data.version) data[e.key] = e;
    else if (it->second.data.version == e.data.version && !SamePayload(it->second.data, e.data))
        throw std::runtime_error("equal version with different payloads");
}
}
Router::Router(const std::vector<std::string>& nodes, int v, int n, int w, int r, bool verbose)
    : Router(nodes, LegacyOptions(v, n, w, r, verbose)) {}
Router::Router(const std::vector<std::string>& nodes, RouterOptions options)
    : options_(std::move(options)), hints_(options_.state_directory, options_.hint_ttl_ms),
      clock_(Identity()), pool_(options_.workers, options_.queue_capacity) {
    const int n = options_.replication_factor, w = options_.write_quorum, r = options_.read_quorum;
    if (n < 1 || w < 1 || r < 1 || w > n || r > n || w + r <= n ||
        options_.operation_timeout.count() <= 0 || options_.replica_timeout.count() <= 0 ||
        options_.repair_interval.count() < 0)
        throw std::invalid_argument("require 1<=W,R<=N, W+R>N and positive deadlines");
    auto members = nodes; uint64_t epoch = 1;
    if (!options_.state_directory.empty()) {
        clock_journal_ = std::make_unique<Journal>(options_.state_directory, "clock");
        clock_journal_->Recover([this](const std::string& bytes) { clock_.Observe(DecodeEntry(bytes).data.version); });
        membership_journal_ = std::make_unique<Journal>(options_.state_directory, "membership");
        membership_journal_->Recover([&](const std::string& bytes) {
            size_t p = 0; epoch = binary::U64(bytes, p);
            auto count = binary::U64(bytes, p);
            if (count > 1024) throw std::runtime_error("invalid membership");
            members.clear();
            for (size_t i = 0; i < count; ++i) members.push_back(binary::String(bytes, p));
            if (p != bytes.size()) throw std::runtime_error("invalid membership record");
        });
    }
    topology_ = BuildTopology(members, epoch);
    // Restore HLC causality from available replicas, including after a router state reset.
    std::map<std::string, Entry> data;
    Scan(topology_, data, false);
    for (const auto& e : data) clock_.Observe(e.second.data.version);
    SaveMembership(*topology_);
    topology_->detector->Start();
    background_ = std::thread([this] { Background(); });
}
Router::~Router() {
    stop_ = true; background_wake_.notify_all();
    if (background_.joinable()) background_.join();
    pool_.Drain();
    topology_->detector->Stop();
}
std::shared_ptr<Router::Topology> Router::BuildTopology(const std::vector<std::string>& nodes, uint64_t epoch) {
    auto t = std::make_shared<Topology>(options_.virtual_nodes);
    if (nodes.size() < static_cast<size_t>(options_.replication_factor))
        throw std::invalid_argument("membership smaller than replication factor");
    if (nodes.size() > 1024) throw std::invalid_argument("too many nodes");
    t->nodes = nodes; t->epoch = epoch;
    for (const auto& addr : nodes) {
        if (addr.find(':') == std::string::npos || !std::all_of(addr.begin(), addr.end(), [](unsigned char c) {
                return std::isalnum(c) || c == '.' || c == ':' || c == '-' || c == '_' || c == '[' || c == ']';
            })) throw std::invalid_argument("invalid node address");
        if (t->stubs.count(addr)) throw std::invalid_argument("duplicate member");
        t->ring.AddNode(addr);
        t->stubs[addr] = std::shared_ptr<Stub>(kvstore::KVStoreService::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())));
    }
    t->detector = std::make_shared<FailureDetector>(nodes); return t;
}
Version Router::NewVersion() {
    std::lock_guard<std::mutex> lock(clock_mutex_);
    auto v = clock_.Next();
    if (clock_journal_) {
        clock_journal_->Append(EncodeEntry({"clock", {"", v.physical_ms, v, false}}), true);
        if (++clock_records_ >= 10000) {
            clock_journal_->Checkpoint({EncodeEntry({"clock", {"", v.physical_ms, v, false}})});
            clock_records_ = 0;
        }
    }
    return v;
}
Entry Router::NewEntry(const std::string& key, const std::string& value, bool tombstone) {
    auto v = NewVersion(); return {key, {tombstone ? "" : value, v.physical_ms, v, tombstone}};
}
std::vector<std::string> Router::ReplicaAddressesFor(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(topology_mutex_);
    return topology_->ring.GetNodesForKey(key, options_.replication_factor);
}
uint64_t Router::Epoch() const { std::shared_lock<std::shared_mutex> lock(topology_mutex_); return topology_->epoch; }
void Router::RecordRPC(const grpc::Status& status) {
    if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED) {
        ++metrics_.rpc_failures;
        if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) ++metrics_.timeouts;
    }
}
void Router::Log(const char* op, const std::string& key, bool ok, const Version* v) const {
    if (!options_.verbose) return;
    std::ostringstream s;
    s << "{\"operation\":\"" << op << "\",\"key_hash\":" << ring_hash(key)
      << ",\"success\":" << (ok ? "true" : "false");
    if (v) s << ",\"physical_ms\":" << v->physical_ms << ",\"logical\":" << v->logical;
    s << "}\n"; std::clog << s.str();
}
WriteResult Router::Mutate(const Entry& entry, Deadline parent_deadline) {
    auto start = std::chrono::steady_clock::now(); ++metrics_.writes;
    std::shared_lock<std::shared_mutex> gate(topology_mutex_);
    auto topology = topology_;
    auto replicas = topology->ring.GetNodesForKey(entry.key, options_.replication_factor);
    if (entry.data.version.writer.empty() || entry.key.size() > 4096 || entry.data.value.size() > 1024 * 1024 ||
        (entry.data.tombstone && !entry.data.value.empty()))
        throw std::invalid_argument("invalid mutation/version or payload too large");
    clock_.Observe(entry.data.version);
    struct State {
        std::mutex mutex; std::condition_variable ready;
        int completed = 0, successes = 0; bool stale = false, conflict = false, applied = false;
    };
    auto state = std::make_shared<State>();
    auto deadline = std::min(parent_deadline, std::chrono::system_clock::now() + options_.operation_timeout);
    for (const auto& node : replicas) {
        auto job = [this, topology, state, node, entry, deadline] {
            bool ok = false, stale = false, conflict = false, applied = false;
            try {
                if (!topology->detector->IsAlive(node)) hints_.Store(node, entry);
                else {
                    auto request = PutRequestFor(entry); kvstore::PutResponse response;
                    grpc::ClientContext context; context.set_deadline(RPCDeadline(options_, deadline));
                    auto status = topology->stubs.at(node)->Put(&context, request, &response); RecordRPC(status);
                    if (response.has_current_version()) clock_.Observe(FromWire(response.current_version()));
                    ok = status.ok() && response.success() &&
                        (response.result() == kvstore::APPLIED || response.result() == kvstore::DUPLICATE);
                    applied = ok && response.result() == kvstore::APPLIED;
                    stale = status.ok() && response.result() == kvstore::STALE;
                    conflict = status.ok() && response.result() == kvstore::CONFLICT;
                    if (!status.ok()) hints_.Store(node, entry);
                }
            } catch (...) { ++metrics_.rpc_failures; }
            if (conflict) ++metrics_.conflicts;
            { std::lock_guard<std::mutex> lock(state->mutex);
              ++state->completed; state->successes += ok; state->stale |= stale;
              state->conflict |= conflict; state->applied |= applied; }
            state->ready.notify_all();
        };
        if (options_.sequential) job();
        else if (!pool_.Submit(job)) {
            ++metrics_.rejected;
            try { hints_.Store(node, entry); } catch (...) { ++metrics_.rpc_failures; }
            std::lock_guard<std::mutex> lock(state->mutex); ++state->completed; state->ready.notify_all();
        }
    }
    std::unique_lock<std::mutex> lock(state->mutex);
    state->ready.wait_until(lock, deadline, [&] {
        return state->successes >= options_.write_quorum || state->completed == static_cast<int>(replicas.size());
    });
    bool ok = state->successes >= options_.write_quorum;
    auto result = ok ? (state->applied ? kvstore::APPLIED : kvstore::DUPLICATE) : state->conflict ? kvstore::CONFLICT :
        state->stale ? kvstore::STALE : kvstore::QUORUM_FAILED;
    if (!ok) ++metrics_.failures;
    metrics_.Observe(start); Log(entry.data.tombstone ? "delete" : "put", entry.key, ok, &entry.data.version);
    return {ok, result, entry.data.version};
}
bool Router::Put(const std::string& key, const std::string& value) { return Mutate(NewEntry(key, value)).success; }
bool Router::Remove(const std::string& key) { return Mutate(NewEntry(key, "", true)).success; }
ReadResult Router::Read(const std::string& key, Deadline parent_deadline) {
    auto start = std::chrono::steady_clock::now(); ++metrics_.reads;
    std::shared_lock<std::shared_mutex> gate(topology_mutex_);
    auto topology = topology_;
    auto replicas = topology->ring.GetNodesForKey(key, options_.replication_factor);
    struct State {
        std::mutex mutex; std::condition_variable ready;
        int completed = 0, replies = 0;
        std::map<std::string, std::optional<TimestampedValue>> values;
    };
    auto state = std::make_shared<State>();
    auto deadline = std::min(parent_deadline, std::chrono::system_clock::now() + options_.operation_timeout);
    std::vector<std::shared_ptr<grpc::ClientContext>> contexts;
    for (const auto& node : replicas) {
        auto context = std::make_shared<grpc::ClientContext>();
        context->set_deadline(RPCDeadline(options_, deadline)); contexts.push_back(context);
        auto job = [this, topology, state, node, key, context] {
            bool replied = false; std::optional<TimestampedValue> value;
            try {
                if (topology->detector->IsAlive(node)) {
                    kvstore::GetRequest request; request.set_key(key); kvstore::GetResponse response;
                    auto status = topology->stubs.at(node)->Get(context.get(), request, &response); RecordRPC(status);
                    replied = status.ok();
                    if (replied && response.has_version())
                        value = TimestampedValue{response.value(), response.timestamp(), FromWire(response.version()), response.tombstone()};
                }
            } catch (...) { ++metrics_.rpc_failures; }
            { std::lock_guard<std::mutex> lock(state->mutex);
              ++state->completed;
              if (replied) { ++state->replies; state->values[node] = value; } }
            state->ready.notify_all();
        };
        if (options_.sequential) job();
        else if (!pool_.Submit(job)) {
            ++metrics_.rejected; std::lock_guard<std::mutex> lock(state->mutex);
            ++state->completed; state->ready.notify_all();
        }
    }
    std::unique_lock<std::mutex> lock(state->mutex);
    state->ready.wait_until(lock, deadline, [&] {
        return state->replies >= options_.read_quorum || state->completed == static_cast<int>(replicas.size());
    });
    ReadResult result;
    if (state->replies < options_.read_quorum) { ++metrics_.failures; result.status = ReadStatus::Unavailable; }
    else {
        result.status = ReadStatus::NotFound;
        for (const auto& item : state->values) {
            const auto& value = item.second;
            if (!value) continue;
            if (!result.data || result.data->version < value->version) result.data = value;
            else if (result.data->version == value->version && !SamePayload(*result.data, *value)) {
                result.status = ReadStatus::Conflict; ++metrics_.conflicts; break;
            }
        }
        if (result.status != ReadStatus::Conflict && result.data)
            result.status = result.data->tombstone ? ReadStatus::NotFound : ReadStatus::Found;
    }
    // Unneeded reads are cancelled; writes intentionally continue bounded replication after quorum.
    auto observed = state->values;
    lock.unlock();
    for (const auto& context : contexts) context->TryCancel();
    if (result.data && result.status != ReadStatus::Conflict && result.status != ReadStatus::Unavailable) {
        clock_.Observe(result.data->version);
        for (const auto& node : replicas) {
            auto it = observed.find(node);
            if (it != observed.end() && it->second && !(it->second->version < result.data->version)) continue;
            Entry repair{key, *result.data};
            if (!pool_.Submit([this, node, repair] {
                try { hints_.Store(node, repair); ++metrics_.repairs; }
                catch (...) { ++metrics_.rpc_failures; }
            })) ++metrics_.rejected;
        }
    }
    metrics_.Observe(start); Log("get", key, result.status == ReadStatus::Found);
    return result;
}
bool Router::Get(const std::string& key, std::string* out) {
    if (!out) throw std::invalid_argument("null output");
    auto result = Read(key);
    if (result.status != ReadStatus::Found) return false;
    *out = result.data->value; return true;
}
void Router::ReplayLocked(const std::shared_ptr<Topology>& topology) {
    std::lock_guard<std::mutex> replay(replay_mutex_);
    for (const auto& hint : hints_.Due()) {
        if (stop_) return;
        try {
            auto it = topology->stubs.find(hint.node);
            if (it == topology->stubs.end() || !topology->detector->IsAlive(hint.node)) {
                hints_.Failed(hint); continue;
            }
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + options_.replica_timeout);
            auto request = PutRequestFor(hint.entry); kvstore::PutResponse response;
            auto status = it->second->Put(&context, request, &response); RecordRPC(status);
            // STALE is safe here: the replica already holds a strictly newer version.
            if (status.ok() && (response.success() || response.result() == kvstore::STALE)) {
                hints_.Complete(hint); ++metrics_.hints_replayed;
            } else hints_.Failed(hint);
        } catch (...) { ++metrics_.rpc_failures; hints_.Failed(hint); }
    }
}
void Router::ReplayHintsForRecoveredNodes() {
    std::shared_lock<std::shared_mutex> gate(topology_mutex_); ReplayLocked(topology_);
}
bool Router::Scan(const std::shared_ptr<Topology>& topology, std::map<std::string, Entry>& data, bool strict) {
    bool all = true;
    for (const auto& node : topology->nodes) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + options_.operation_timeout);
        kvstore::ScanRequest request;
        auto reader = topology->stubs.at(node)->Scan(&context, request);
        kvstore::Record record; std::map<std::string, Entry> received;
        while (reader->Read(&record)) {
            if (received.size() >= 100000) { context.TryCancel(); break; }
            Merge(received, FromWire(record));
        }
        auto status = reader->Finish(); RecordRPC(status);
        if (!status.ok()) { all = false; if (strict) return false; continue; }
        for (const auto& item : received) Merge(data, item.second);
    }
    return all;
}
bool Router::RepairLocked(const std::shared_ptr<Topology>& topology) {
    std::map<std::string, Entry> data;
    bool all = Scan(topology, data, false);
    for (const auto& item : data) {
        clock_.Observe(item.second.data.version);
        for (const auto& node : topology->ring.GetNodesForKey(item.first, options_.replication_factor))
            hints_.Store(node, item.second);
    }
    ReplayLocked(topology); return all;
}
bool Router::RepairOnce() {
    std::shared_lock<std::shared_mutex> gate(topology_mutex_); return RepairLocked(topology_);
}
void Router::Background() {
    auto next_repair = std::chrono::steady_clock::now() + options_.repair_interval;
    while (!stop_) {
        try {
            std::shared_lock<std::shared_mutex> gate(topology_mutex_);
            ReplayLocked(topology_);
            if (options_.repair_interval.count() && std::chrono::steady_clock::now() >= next_repair) {
                RepairLocked(topology_);
                next_repair = std::chrono::steady_clock::now() + options_.repair_interval;
            }
        } catch (const std::exception& e) { std::clog << "background repair error: " << e.what() << "\n"; }
        std::unique_lock<std::mutex> lock(background_mutex_);
        background_wake_.wait_for(lock, std::chrono::milliseconds(100), [this] { return stop_.load(); });
    }
}
void Router::SaveMembership(const Topology& topology) {
    if (!membership_journal_) return;
    std::string bytes; binary::U64(bytes, topology.epoch); binary::U64(bytes, topology.nodes.size());
    for (const auto& node : topology.nodes) binary::String(bytes, node);
    membership_journal_->Checkpoint({bytes});
}
bool Router::Reconfigure(const std::vector<std::string>& nodes, uint64_t epoch, std::string* error) {
    // Stop admission, drain late writes, copy all versions, then durably activate the new ring.
    std::unique_lock<std::shared_mutex> gate(topology_mutex_);
    try {
        if (epoch <= topology_->epoch) throw std::invalid_argument("epoch must increase");
        auto next = BuildTopology(nodes, epoch);
        pool_.Drain();
        std::map<std::string, Entry> data;
        if (!Scan(topology_, data, true)) throw std::runtime_error("all old nodes must be reachable for safe migration");
        for (const auto& h : hints_.All()) Merge(data, h.entry);
        // Read new members too, so pre-existing data cannot be silently overwritten.
        if (!Scan(next, data, true)) throw std::runtime_error("all new nodes must be reachable");
        for (const auto& item : data) {
            for (const auto& node : next->ring.GetNodesForKey(item.first, options_.replication_factor)) {
                auto request = PutRequestFor(item.second); kvstore::PutResponse response;
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + options_.replica_timeout);
                auto status = next->stubs.at(node)->Put(&context, request, &response); RecordRPC(status);
                if (!status.ok() || !response.success()) throw std::runtime_error("replica migration failed; old ring stays active");
            }
            clock_.Observe(item.second.data.version);
        }
        next->detector->Start();
        SaveMembership(*next);
        auto previous = topology_; topology_ = next; previous->detector->Stop();
        ++metrics_.migrations;
        try { hints_.DropTargetsExcept(nodes); }
        catch (const std::exception& e) { std::clog << "membership committed; hint cleanup will need recovery: " << e.what() << "\n"; }
        return true;
    } catch (const std::exception& e) { if (error) *error = e.what(); return false; }
}
std::string Router::MetricsText() const {
    std::shared_lock<std::shared_mutex> gate(topology_mutex_);
    auto text = metrics_.Text();
    text += "kv_hints_pending " + std::to_string(hints_.size()) + "\n";
    text += "kv_hints_expired_total " + std::to_string(hints_.expired()) + "\n";
    text += "kv_ring_epoch " + std::to_string(topology_->epoch) + "\n";
    for (const auto& node : topology_->nodes)
        text += "kv_node_liveness{node=\"" + node + "\"} " +
            (topology_->detector->IsAlive(node) ? "1\n" : "0\n");
    return text;
}
