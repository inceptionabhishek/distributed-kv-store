#pragma once
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <future>
#include <vector>

// Bounded admission: overload is explicit; no detached threads or unbounded queue.
class ThreadPool {
public:
    explicit ThreadPool(size_t threads = 16, size_t capacity = 4096) : capacity_(capacity) {
        if (!threads || !capacity) throw std::invalid_argument("invalid worker pool");
        try {
        for (size_t i = 0; i < threads; ++i) workers_.emplace_back([this] {
            for (;;) {
                std::function<void()> job;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    ready_.wait(lock, [this] { return stop_ || !jobs_.empty(); });
                    if (stop_ && jobs_.empty()) return;
                    job = std::move(jobs_.front()); jobs_.pop_front(); ++active_;
                }
                try { job(); } catch (...) { /* jobs report errors to their own operation state */ }
                {
                    std::lock_guard<std::mutex> lock(mutex_); --active_;
                    if (!active_ && jobs_.empty()) idle_.notify_all();
                }
            }
        });
        } catch (...) {
            { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
            ready_.notify_all();
            for (auto& worker : workers_) worker.join();
            throw;
        }
    }
    ~ThreadPool() {
        { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
        ready_.notify_all(); for (auto& worker : workers_) worker.join();
    }
    bool Submit(std::function<void()> job) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_ || jobs_.size() >= capacity_) return false;
        jobs_.push_back(std::move(job)); ready_.notify_one(); return true;
    }
    void Drain() {
        std::unique_lock<std::mutex> lock(mutex_);
        idle_.wait(lock, [this] { return jobs_.empty() && active_ == 0; });
    }
private:
    std::mutex mutex_;
    std::condition_variable ready_, idle_;
    std::deque<std::function<void()>> jobs_;
    std::vector<std::thread> workers_;
    size_t capacity_, active_ = 0;
    bool stop_ = false;
};
