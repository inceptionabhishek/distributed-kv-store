#pragma once
#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

class FailureDetector {
public:
    FailureDetector(std::vector<std::string> addresses,
                    std::chrono::milliseconds interval = std::chrono::milliseconds(250),
                    std::chrono::milliseconds timeout = std::chrono::milliseconds(100))
        : interval_(interval), timeout_(timeout) {
        for (const auto& address : addresses) {
            stubs_[address] = kvstore::KVStoreService::NewStub(
                grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
            alive_[address] = true;
        }
    }
    ~FailureDetector() { Stop(); }
    void Start() {
        std::lock_guard<std::mutex> lock(lifecycle_);
        if (worker_.joinable()) return;
        running_ = true; worker_ = std::thread([this] { Run(); });
    }
    void Stop() {
        std::lock_guard<std::mutex> lock(lifecycle_);
        running_ = false; wake_.notify_all();
        if (worker_.joinable()) worker_.join();
    }
    bool IsAlive(const std::string& address) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = alive_.find(address); return it != alive_.end() && it->second;
    }
    void PrintStatus() const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& item : alive_) std::cout << item.first << ": " << (item.second ? "UP" : "DOWN") << "\n";
    }
private:
    void Run() {
        while (running_) {
            for (const auto& item : stubs_) {
                if (!running_) break;
                kvstore::PingRequest request; kvstore::PingResponse response;
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + timeout_);
                auto status = item.second->Ping(&context, request, &response);
                std::lock_guard<std::mutex> lock(mutex_);
                alive_[item.first] = status.ok() && response.alive();
            }
            std::unique_lock<std::mutex> lock(sleep_mutex_);
            wake_.wait_for(lock, interval_, [this] { return !running_; });
        }
    }
    std::chrono::milliseconds interval_, timeout_;
    std::map<std::string, std::unique_ptr<kvstore::KVStoreService::Stub>> stubs_;
    mutable std::mutex mutex_;
    std::map<std::string, bool> alive_;
    std::mutex lifecycle_, sleep_mutex_;
    std::condition_variable wake_;
    std::atomic<bool> running_{false};
    std::thread worker_;
};
