#include "runtime.hpp"
#include "wire.hpp"
#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>

// Retry with --version physical:logical:writer and the identical payload.
Version ParseVersion(const std::string& text) {
    auto first = text.find(':'), second = text.find(':', first == std::string::npos ? 0 : first + 1);
    if (first == std::string::npos || second == std::string::npos) throw std::invalid_argument("invalid version token");
    Version v{std::stoull(text.substr(0, first)), std::stoull(text.substr(first + 1, second - first - 1)), text.substr(second + 1)};
    if (v.writer.empty()) throw std::invalid_argument("empty writer"); return v;
}
std::string Token(const Version& v) { return std::to_string(v.physical_ms) + ":" + std::to_string(v.logical) + ":" + v.writer; }
int main(int argc, char** argv) {
    try {
        Arguments args(argc, argv); auto p = args.positional;
        if (p.empty()) throw std::invalid_argument("usage: kvctl [--address host:port] put|get|delete|replicas|scan|metrics|repair|snapshot|reconfigure ...");
        auto stub = kvstore::KVStoreService::NewStub(grpc::CreateChannel(args.Get("address", "localhost:50050"), grpc::InsecureChannelCredentials()));
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(args.Int("timeout-ms", 10000)));
        grpc::Status status;
        auto require = [&](size_t n) { if (p.size() != n) throw std::invalid_argument("wrong number of arguments"); };
        if (p[0] == "put" || p[0] == "delete") {
            require(p[0] == "put" ? 3 : 2);
            Version v;
            if (args.flags.count("version")) v = ParseVersion(args.Get("version", ""));
            else {
                grpc::ClientContext reserve_context;
                reserve_context.set_deadline(context.deadline());
                kvstore::PingRequest reserve; kvstore::Version reserved;
                auto reserved_status = stub->Reserve(&reserve_context, reserve, &reserved);
                if (!reserved_status.ok()) throw std::runtime_error(reserved_status.error_message());
                v = FromWire(reserved);
            }
            // Print before sending so an ambiguous network failure can be retried safely.
            std::cout << "version=" << Token(v) << std::endl;
            auto request = PutRequestFor({p[1], {p[0] == "put" ? p[2] : "", v.physical_ms, v, p[0] == "delete"}});
            kvstore::PutResponse response; status = stub->Put(&context, request, &response);
            if (!status.ok()) throw std::runtime_error(status.error_message());
            std::cout << kvstore::WriteStatus_Name(response.result()) << "\n";
            return response.success() ? 0 : 2;
        } else if (p[0] == "get") {
            require(2); kvstore::GetRequest request; request.set_key(p[1]); kvstore::GetResponse response;
            status = stub->Get(&context, request, &response);
            if (status.ok()) {
                if (!response.found()) { std::cout << "NOT_FOUND\n"; return 3; }
                std::cout << response.value() << "\nversion=" << Token(FromWire(response.version())) << "\n";
            }
        } else if (p[0] == "replicas") {
            require(2); kvstore::DebugRequest request; request.set_key(p[1]); kvstore::DebugResponse response;
            status = stub->Debug(&context, request, &response);
            if (status.ok()) { std::cout << "epoch=" << response.epoch() << "\n"; for (const auto& node : response.replicas()) std::cout << node << "\n"; }
        } else if (p[0] == "scan") {
            require(1); kvstore::ScanRequest request; auto reader = stub->Scan(&context, request); kvstore::Record record;
            while (reader->Read(&record))
                std::cout << record.key() << "\t" << (record.tombstone() ? "<TOMBSTONE>" : record.value()) << "\t" << Token(FromWire(record.version())) << "\n";
            status = reader->Finish();
        } else if (p[0] == "metrics") {
            require(1); kvstore::MetricsRequest request; kvstore::MetricsResponse response;
            status = stub->Metrics(&context, request, &response); if (status.ok()) std::cout << response.text();
        } else {
            kvstore::AdminResponse response;
            if (p[0] == "snapshot") { require(1); kvstore::SnapshotRequest request; status = stub->Snapshot(&context, request, &response); }
            else if (p[0] == "repair") { require(1); kvstore::PingRequest request; status = stub->Repair(&context, request, &response); }
            else if (p[0] == "reconfigure") {
                require(3); kvstore::ReconfigureRequest request; request.set_epoch(std::stoull(p[1]));
                for (const auto& node : SplitNodes(p[2])) request.add_nodes(node);
                status = stub->Reconfigure(&context, request, &response);
            } else throw std::invalid_argument("unknown command");
            if (status.ok()) { std::cout << response.detail() << "\n"; return response.success() ? 0 : 2; }
        }
        if (!status.ok()) throw std::runtime_error(status.error_message());
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << "\n"; return 1; }
}
