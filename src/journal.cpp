#include "journal.hpp"
#include "simple_hash.hpp"
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <chrono>

namespace {
constexpr uint64_t kMaxRecord = 16 * 1024 * 1024;
void Check(bool ok, const char* what) {
    if (!ok) throw std::runtime_error(std::string(what) + ": " + std::strerror(errno));
}
void WriteAll(int fd, const std::string& data) {
    size_t offset = 0;
    while (offset < data.size()) {
        auto n = ::write(fd, data.data() + offset, data.size() - offset);
        if (n < 0 && errno == EINTR) continue;
        Check(n > 0, "write journal"); offset += static_cast<size_t>(n);
    }
}
std::string Frame(const std::string& payload) {
    if (payload.size() > kMaxRecord) throw std::runtime_error("record too large");
    std::string frame;
    binary::U64(frame, payload.size()); binary::U64(frame, fnv1a_hash(payload));
    return frame + payload;
}
off_t Replay(const std::string& path, bool allow_tail,
             const std::function<void(const std::string&)>& consume) {
    if (!std::filesystem::exists(path)) return 0;
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot read " + path);
    off_t valid = 0;
    for (;;) {
        std::string header(16, '\0');
        in.read(header.data(), 16);
        auto got = in.gcount();
        if (got == 0 && in.eof()) return valid;
        if (got != 16) {
            if (allow_tail && in.eof()) return valid;
            throw std::runtime_error("incomplete snapshot/header: " + path);
        }
        size_t p = 0;
        const std::string& input = header;
        auto length = binary::U64(input, p), checksum = binary::U64(input, p);
        if (length > kMaxRecord) throw std::runtime_error("invalid journal length: " + path);
        std::string payload(length, '\0');
        in.read(payload.data(), static_cast<std::streamsize>(length));
        if (static_cast<uint64_t>(in.gcount()) != length) {
            if (allow_tail && in.eof()) return valid;
            throw std::runtime_error("incomplete snapshot: " + path);
        }
        if (fnv1a_hash(payload) != checksum) throw std::runtime_error("journal checksum mismatch: " + path);
        consume(payload); valid += static_cast<off_t>(16 + length);
    }
}
void SyncDirectory(const std::string& directory) {
    int fd = ::open(directory.c_str(), O_RDONLY);
    Check(fd >= 0, "open directory");
    int rc = ::fsync(fd); ::close(fd); Check(rc == 0, "sync directory");
}
}
namespace binary {
void U64(std::string& out, uint64_t n) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((n >> (i * 8)) & 255));
}
uint64_t U64(const std::string& in, size_t& p) {
    if (p > in.size() || in.size() - p < 8) throw std::runtime_error("invalid record integer");
    uint64_t n = 0;
    for (int i = 0; i < 8; ++i) n |= static_cast<uint64_t>(static_cast<unsigned char>(in[p++])) << (i * 8);
    return n;
}
void String(std::string& out, const std::string& s) { U64(out, s.size()); out += s; }
std::string String(const std::string& in, size_t& p) {
    auto size = U64(in, p);
    if (size > in.size() - p) throw std::runtime_error("invalid record string");
    auto result = in.substr(p, size); p += size; return result;
}
}
Journal::Journal(const std::string& directory, const std::string& name)
    : directory_(directory), wal_(directory + "/" + name + ".wal"), snapshot_(directory + "/" + name + ".snapshot") {
    std::filesystem::create_directories(directory);
    lock_fd_ = ::open((directory + "/" + name + ".lock").c_str(), O_CREAT | O_RDWR, 0600);
    if (lock_fd_ < 0) throw std::runtime_error("cannot open directory lock");
    if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        ::close(lock_fd_); lock_fd_ = -1; throw std::runtime_error("data directory already in use");
    }
    fd_ = ::open(wal_.c_str(), O_CREAT | O_RDWR | O_APPEND, 0600);
    if (fd_ < 0) { ::close(lock_fd_); throw std::runtime_error("cannot open WAL"); }
    try { SyncDirectory(directory_); }
    catch (...) { ::close(fd_); ::close(lock_fd_); throw; }
}
Journal::~Journal() { if (fd_ >= 0) ::close(fd_); if (lock_fd_ >= 0) ::close(lock_fd_); }
void Journal::Recover(const std::function<void(const std::string&)>& consume) {
    Replay(snapshot_, false, consume);
    auto length = Replay(wal_, true, consume);
    Check(::ftruncate(fd_, length) == 0, "truncate torn WAL tail"); Sync();
}
void Journal::Append(const std::string& payload, bool sync) {
    if (poisoned_) throw std::runtime_error("journal poisoned by previous IO failure");
    try { WriteAll(fd_, Frame(payload)); if (sync) Sync(); }
    catch (...) { poisoned_ = true; throw; }
}
void Journal::Sync() {
    if (poisoned_) throw std::runtime_error("journal poisoned by previous IO failure");
    auto start = std::chrono::steady_clock::now();
    if (::fsync(fd_) != 0) { poisoned_ = true; Check(false, "fsync WAL"); }
    ++sync_count_; sync_us_ += std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
}
void Journal::Checkpoint(const std::vector<std::string>& records) {
    if (poisoned_) throw std::runtime_error("journal poisoned");
    auto temp = snapshot_ + ".tmp";
    int fd = ::open(temp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
    Check(fd >= 0, "open snapshot");
    try {
        for (const auto& record : records) WriteAll(fd, Frame(record));
        Check(::fsync(fd) == 0, "fsync snapshot"); ::close(fd); fd = -1;
        Check(::rename(temp.c_str(), snapshot_.c_str()) == 0, "rename snapshot");
        SyncDirectory(directory_);
        Check(::ftruncate(fd_, 0) == 0, "rotate WAL"); Sync();
    } catch (...) { if (fd >= 0) ::close(fd); poisoned_ = true; throw; }
}
