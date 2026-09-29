#include "hint_queue.hpp"
#include <algorithm>
#include <stdexcept>
namespace {
std::string EncodeHint(bool erase, const Hint& h) {
    std::string bytes;
    binary::U64(bytes, erase); binary::String(bytes, h.node);
    binary::U64(bytes, h.created_ms); binary::String(bytes, EncodeEntry(h.entry)); return bytes;
}
}
HintQueue::HintQueue(const std::string& directory, uint64_t ttl_ms) : ttl_ms_(ttl_ms) {
    if (!directory.empty()) {
        journal_ = std::make_unique<Journal>(directory, "hints");
        journal_->Recover([this](const std::string& bytes) {
            size_t p = 0; auto erase = binary::U64(bytes, p); Hint h;
            h.node = binary::String(bytes, p); h.created_ms = binary::U64(bytes, p);
            h.entry = DecodeEntry(binary::String(bytes, p));
            if (p != bytes.size() || erase > 1) throw std::runtime_error("invalid hint journal");
            Key key{h.node, h.entry.key};
            if (erase) hints_.erase(key); else hints_[key] = std::move(h);
        });
    }
}
void HintQueue::Persist(bool erase, const Hint& h) {
    if (journal_) journal_->Append(EncodeHint(erase, h), true);
}
void HintQueue::CompactLocked() {
    if (journal_ && ++changes_ >= 1000) {
        std::vector<std::string> records;
        for (const auto& h : hints_) records.push_back(EncodeHint(false, h.second));
        journal_->Checkpoint(records); changes_ = 0;
    }
}
void HintQueue::Store(const std::string& node, const Entry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{node, entry.key}; auto it = hints_.find(key);
    if (it != hints_.end() && !(it->second.entry.data.version < entry.data.version)) {
        if (it->second.entry.data.version == entry.data.version &&
            !SamePayload(it->second.entry.data, entry.data)) throw std::runtime_error("hint version conflict");
        return;
    }
    Hint h{node, entry, WallMillis(), 0, 0};
    Persist(false, h); hints_[key] = h; CompactLocked();
}
std::vector<Hint> HintQueue::Due() const {
    std::lock_guard<std::mutex> lock(mutex_); std::vector<Hint> result;
    auto now = WallMillis();
    for (const auto& h : hints_) if (h.second.next_retry_ms <= now) result.push_back(h.second);
    return result;
}
std::vector<Hint> HintQueue::All() const {
    std::lock_guard<std::mutex> lock(mutex_); std::vector<Hint> result;
    for (const auto& h : hints_) result.push_back(h.second);
    return result;
}
void HintQueue::Complete(const Hint& h) {
    std::lock_guard<std::mutex> lock(mutex_); auto it = hints_.find({h.node, h.entry.key});
    // Don't erase a newer mutation inserted while this replay was in flight.
    if (it == hints_.end() || it->second.entry.data.version != h.entry.data.version) return;
    Persist(true, h); hints_.erase(it); CompactLocked();
}
void HintQueue::Failed(const Hint& h) {
    std::lock_guard<std::mutex> lock(mutex_); auto it = hints_.find({h.node, h.entry.key});
    if (it == hints_.end() || it->second.entry.data.version != h.entry.data.version) return;
    if (ttl_ms_ && WallMillis() - std::min(WallMillis(), h.created_ms) >= ttl_ms_) {
        Persist(true, h); hints_.erase(it); ++expired_; CompactLocked(); return;
    }
    auto& pending = it->second; ++pending.attempts;
    pending.next_retry_ms = WallMillis() + std::min<uint64_t>(30000, 100ULL << std::min(pending.attempts, 8u));
}
void HintQueue::DropTargetsExcept(const std::vector<std::string>& nodes) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = hints_.begin(); it != hints_.end();) {
        if (std::find(nodes.begin(), nodes.end(), it->first.first) == nodes.end()) {
            Persist(true, it->second); it = hints_.erase(it);
        } else ++it;
    }
    CompactLocked();
}
size_t HintQueue::size() const { std::lock_guard<std::mutex> lock(mutex_); return hints_.size(); }
uint64_t HintQueue::expired() const { std::lock_guard<std::mutex> lock(mutex_); return expired_; }
void HintQueue::Snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!journal_) return;
    std::vector<std::string> records;
    for (const auto& h : hints_) records.push_back(EncodeHint(false, h.second));
    journal_->Checkpoint(records); changes_ = 0;
}
