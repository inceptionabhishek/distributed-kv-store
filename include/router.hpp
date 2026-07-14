#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <mutex>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include "consistent_hash_ring.hpp"
#include "failure_detector.hpp"

struct Hint {
    std::string key;
    std::string value;
    uint64_t timestamp;
    std::string intended_node;
};

class Router {
public:
    explicit Router(const std::vector<std::string>& node_addresses,
                     int virtual_nodes = 10,
                     int replication_factor = 3,
                     int write_quorum = 2,
                     int read_quorum = 2,
                     bool verbose = true)
        : ring_(virtual_nodes),
          replication_factor_(replication_factor),
          write_quorum_(write_quorum),
          read_quorum_(read_quorum),
          failure_detector_(node_addresses),
          verbose_(verbose) {
        for (const auto& addr : node_addresses) {
            ring_.AddNode(addr);
            stubs_[addr] = kvstore::KVStoreService::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        }
        failure_detector_.Start();
    }

    ~Router() {
        failure_detector_.Stop();
    }

    std::vector<std::string> ReplicaAddressesFor(const std::string& key) const {
        return ring_.GetNodesForKey(key, replication_factor_);
    }

    bool Put(const std::string& key, const std::string& value) {
        auto replicas = ReplicaAddressesFor(key);
        if (static_cast<int>(replicas.size()) < write_quorum_) {
            if (verbose_) std::cerr << "Put failed for key '" << key << "': not enough nodes for write quorum" << std::endl;
            return false;
        }

        uint64_t timestamp = NowMillis();

        if (verbose_) {
            std::cout << "PUT   " << key << " (ts=" << timestamp << ") -> replicas: [";
            for (size_t i = 0; i < replicas.size(); ++i) {
                std::cout << replicas[i] << (i + 1 < replicas.size() ? ", " : "");
            }
            std::cout << "]" << std::endl;
        }

        int successes = 0;
        for (const auto& addr : replicas) {
            if (!failure_detector_.IsAlive(addr)) {
                if (verbose_) std::cout << "  " << addr << " known DOWN, skipping RPC, storing hint" << std::endl;
                StoreHint(addr, {key, value, timestamp, addr});
                continue;
            }

            kvstore::PutRequest request;
            request.set_key(key);
            request.set_value(value);
            request.set_timestamp(timestamp);
            kvstore::PutResponse response;
            grpc::ClientContext context;

            grpc::Status status = stubs_[addr]->Put(&context, request, &response);
            if (status.ok() && response.success()) {
                successes++;
            } else {
                if (verbose_) std::cerr << "  Put to replica " << addr << " FAILED: " << status.error_message() << " -- storing hint" << std::endl;
                StoreHint(addr, {key, value, timestamp, addr});
            }
        }

        bool ok = successes >= write_quorum_;
        if (verbose_) {
            std::cout << "  " << successes << "/" << replicas.size() << " replicas acked "
                       << "(needed " << write_quorum_ << ") -> " << (ok ? "SUCCESS" : "FAILED") << std::endl;
        }
        return ok;
    }

    bool Get(const std::string& key, std::string* out_value) {
        auto replicas = ReplicaAddressesFor(key);
        if (static_cast<int>(replicas.size()) < read_quorum_) {
            if (verbose_) std::cerr << "Get failed for key '" << key << "': not enough nodes for read quorum" << std::endl;
            return false;
        }

        struct Reply { bool found; std::string value; uint64_t timestamp; };
        std::vector<Reply> replies;

        for (const auto& addr : replicas) {
            if (!failure_detector_.IsAlive(addr)) {
                if (verbose_) std::cout << "  " << addr << " known DOWN, skipping" << std::endl;
                continue;
            }

            kvstore::GetRequest request;
            request.set_key(key);
            kvstore::GetResponse response;
            grpc::ClientContext context;

            grpc::Status status = stubs_[addr]->Get(&context, request, &response);
            if (!status.ok()) {
                if (verbose_) std::cerr << "  Get from replica " << addr << " FAILED: " << status.error_message() << std::endl;
                continue;
            }
            replies.push_back({response.found(), response.value(), response.timestamp()});
            if (static_cast<int>(replies.size()) >= read_quorum_) {
                break;
            }
        }

        if (static_cast<int>(replies.size()) < read_quorum_) {
            if (verbose_) std::cerr << "Get failed for key '" << key << "': only got " << replies.size() << "/" << read_quorum_ << " needed replies" << std::endl;
            return false;
        }

        const Reply* best = nullptr;
        for (const auto& r : replies) {
            if (!r.found) continue;
            if (best == nullptr || r.timestamp > best->timestamp) {
                best = &r;
            }
        }

        if (verbose_) {
            std::cout << "GET   " << key << " -> queried " << replies.size()
                       << " replicas (needed " << read_quorum_ << ")" << std::endl;
        }

        if (best == nullptr) {
            return false;
        }
        *out_value = best->value;
        return true;
    }

    void ReplayHintsForRecoveredNodes() {
        std::lock_guard<std::mutex> lock(hints_mutex_);
        for (auto it = hints_.begin(); it != hints_.end(); ) {
            const std::string& node = it->first;
            if (failure_detector_.IsAlive(node)) {
                std::vector<Hint>& pending = it->second;
                if (verbose_) {
                    std::cout << "[hinted-handoff] " << node << " is back UP, replaying "
                               << pending.size() << " hint(s)" << std::endl;
                }
                for (const auto& hint : pending) {
                    kvstore::PutRequest request;
                    request.set_key(hint.key);
                    request.set_value(hint.value);
                    request.set_timestamp(hint.timestamp);
                    kvstore::PutResponse response;
                    grpc::ClientContext context;
                    grpc::Status status = stubs_[node]->Put(&context, request, &response);
                    if (verbose_) {
                        std::cout << "  replayed key='" << hint.key << "' to " << node
                                   << " -> " << (status.ok() ? "ok" : "FAILED") << std::endl;
                    }
                }
                it = hints_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    static uint64_t NowMillis() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    void StoreHint(const std::string& dead_node, Hint hint) {
        std::lock_guard<std::mutex> lock(hints_mutex_);
        hints_[dead_node].push_back(std::move(hint));
    }

    ConsistentHashRing ring_;
    int replication_factor_;
    int write_quorum_;
    int read_quorum_;
    FailureDetector failure_detector_;
    bool verbose_;
    std::unordered_map<std::string, std::unique_ptr<kvstore::KVStoreService::Stub>> stubs_;

    std::mutex hints_mutex_;
    std::unordered_map<std::string, std::vector<Hint>> hints_;
};