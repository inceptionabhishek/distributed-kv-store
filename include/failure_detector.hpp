#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include <vector>
#include <iostream>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

// Runs a background thread that periodically Pings every known node on
// a SHORT deadline (much shorter than a normal RPC timeout) and tracks
// which ones are currently reachable. The router consults this instead
// of blindly firing every Put/Get at every replica and eating a full
// multi-second connection timeout each time a node happens to be down.
class FailureDetector {
public:
    FailureDetector(std::vector<std::string> node_addresses,
                     std::chrono::milliseconds ping_interval = std::chrono::milliseconds(500),
                     std::chrono::milliseconds ping_timeout = std::chrono::milliseconds(200))
        : node_addresses_(std::move(node_addresses)),
          ping_interval_(ping_interval),
          ping_timeout_(ping_timeout) {
        for (const auto& addr : node_addresses_) {
            stubs_[addr] = kvstore::KVStoreService::NewStub(
                grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
            alive_[addr] = true;
        }
    }

    ~FailureDetector() {
        Stop();
    }

    void Start() {
        running_ = true;
        worker_ = std::thread([this]() { this->Run(); });
    }

    void Stop() {
        running_ = false;
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    bool IsAlive(const std::string& address) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = alive_.find(address);
        return it != alive_.end() && it->second;
    }

    void PrintStatus() const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& addr : node_addresses_) {
            std::cout << "  " << addr << ": " << (alive_.at(addr) ? "UP" : "DOWN") << std::endl;
        }
    }

private:
    void Run() {
        while (running_) {
            for (const auto& addr : node_addresses_) {
                bool ok = PingOnce(addr);
                bool was_alive;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    was_alive = alive_[addr];
                    alive_[addr] = ok;
                }
                if (was_alive != ok) {
                    std::cout << "[failure-detector] " << addr << " is now "
                              << (ok ? "UP" : "DOWN") << std::endl;
                }
            }
            std::this_thread::sleep_for(ping_interval_);
        }
    }

    bool PingOnce(const std::string& addr) {
        kvstore::PingRequest request;
        kvstore::PingResponse response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + ping_timeout_);

        grpc::Status status = stubs_[addr]->Ping(&context, request, &response);
        return status.ok() && response.alive();
    }

    std::vector<std::string> node_addresses_;
    std::chrono::milliseconds ping_interval_;
    std::chrono::milliseconds ping_timeout_;

    std::unordered_map<std::string, std::unique_ptr<kvstore::KVStoreService::Stub>> stubs_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, bool> alive_;

    std::atomic<bool> running_{false};
    std::thread worker_;
};