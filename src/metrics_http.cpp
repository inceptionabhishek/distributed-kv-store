#include "metrics_http.hpp"
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <poll.h>
#include <cerrno>
MetricsHttp::MetricsHttp(int port, std::function<std::string()> render) {
    if (!port) return;
    if (port < 1 || port > 65535) throw std::invalid_argument("invalid metrics port");
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) throw std::runtime_error("metrics socket failed");
    int reuse = 1; ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(fd_, 16) != 0) {
        ::close(fd_); fd_ = -1; throw std::runtime_error("metrics port unavailable");
    }
    worker_ = std::thread([this, render] {
        while (!stop_) {
            pollfd p{fd_, POLLIN, 0};
            if (::poll(&p, 1, 100) <= 0) continue;
            int client = ::accept(fd_, nullptr, nullptr);
            if (client < 0) continue;
            timeval timeout{1, 0};
            ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            ::setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#ifdef SO_NOSIGPIPE
            int no_signal = 1; ::setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &no_signal, sizeof(no_signal));
#endif
            char request[1024]; auto n = ::recv(client, request, sizeof(request), 0);
            bool metrics = n > 0 && std::string(request, n).rfind("GET /metrics ", 0) == 0;
            std::string body;
            try { body = metrics ? render() : "not found\n"; } catch (...) { body = "metrics error\n"; }
            std::string response = std::string("HTTP/1.1 ") + (metrics ? "200 OK" : "404 Not Found") +
                "\r\nContent-Type: text/plain; version=0.0.4\r\nConnection: close\r\nContent-Length: " +
                std::to_string(body.size()) + "\r\n\r\n" + body;
            size_t offset = 0;
            while (offset < response.size()) {
#ifdef MSG_NOSIGNAL
                auto sent = ::send(client, response.data() + offset, response.size() - offset, MSG_NOSIGNAL);
#else
                auto sent = ::send(client, response.data() + offset, response.size() - offset, 0);
#endif
                if (sent < 0 && errno == EINTR) continue;
                if (sent <= 0) break;
                offset += sent;
            }
            ::close(client);
        }
    });
}
MetricsHttp::~MetricsHttp() {
    stop_ = true; if (worker_.joinable()) worker_.join(); if (fd_ >= 0) ::close(fd_);
}
