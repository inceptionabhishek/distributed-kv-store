#pragma once
#include <grpcpp/grpcpp.h>
#include <csignal>
#include <pthread.h>
#include <stdexcept>
#include <thread>
inline void BlockShutdownSignals() {
    sigset_t set; sigemptyset(&set); sigaddset(&set, SIGTERM); sigaddset(&set, SIGINT);
    if (pthread_sigmask(SIG_BLOCK, &set, nullptr) != 0) throw std::runtime_error("cannot block shutdown signals");
}
inline void WaitWithSignals(grpc::Server* server) {
    std::thread watcher([server] {
        sigset_t set; sigemptyset(&set); sigaddset(&set, SIGTERM); sigaddset(&set, SIGINT);
        int signal = 0; sigwait(&set, &signal);
        server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    });
    server->Wait(); watcher.join();
}
