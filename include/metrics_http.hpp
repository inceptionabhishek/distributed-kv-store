#pragma once
#include <atomic>
#include <functional>
#include <string>
#include <thread>
class MetricsHttp {
public:
    MetricsHttp(int port, std::function<std::string()> render);
    ~MetricsHttp();
private:
    int fd_ = -1;
    std::atomic<bool> stop_{false};
    std::thread worker_;
};
