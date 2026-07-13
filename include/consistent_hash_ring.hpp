#pragma once

#include <map>
#include <string>
#include <vector>
#include <set>
#include <algorithm>

#include "simple_hash.hpp"

// Consistent hash ring: physical nodes are represented by multiple
// virtual points scattered around a hash ring (0 .. UINT64_MAX).
// A key belongs to whichever virtual point is the first one at or
// after the key's own hash position, walking clockwise (wrapping
// around back to the start if we fall off the end).
class ConsistentHashRing {
public:
    explicit ConsistentHashRing(int virtual_nodes_per_node = 10)
        : virtual_nodes_per_node_(virtual_nodes_per_node) {}

    void AddNode(const std::string& node_address) {
        for (int i = 0; i < virtual_nodes_per_node_; ++i) {
            uint64_t point = ring_hash(node_address + "#" + std::to_string(i));
            ring_[point] = node_address;
        }
        physical_nodes_.insert(node_address);
    }

    void RemoveNode(const std::string& node_address) {
        for (int i = 0; i < virtual_nodes_per_node_; ++i) {
            uint64_t point = ring_hash(node_address + "#" + std::to_string(i));
            ring_.erase(point);
        }
        physical_nodes_.erase(node_address);
    }

    // Returns which physical node owns this key (the "primary").
    std::string GetNodeForKey(const std::string& key) const {
        if (ring_.empty()) {
            return "";
        }
        uint64_t key_hash = ring_hash(key);

        auto it = ring_.lower_bound(key_hash);
        if (it == ring_.end()) {
            it = ring_.begin();
        }
        return it->second;
    }

    // Returns up to `replication_factor` DISTINCT physical nodes for this
    // key, walking clockwise around the ring starting from the key's
    // position. The first entry equals GetNodeForKey() -- the primary
    // owner -- and the rest are its ring successors, which is where
    // replicas live.
    std::vector<std::string> GetNodesForKey(const std::string& key, int replication_factor) const {
        std::vector<std::string> result;
        if (ring_.empty() || replication_factor <= 0) {
            return result;
        }

        uint64_t key_hash = ring_hash(key);
        auto it = ring_.lower_bound(key_hash);

        size_t attempts = 0;
        size_t max_attempts = ring_.size();

        while (result.size() < static_cast<size_t>(replication_factor) && attempts < max_attempts) {
            if (it == ring_.end()) {
                it = ring_.begin();
            }
            const std::string& candidate = it->second;
            if (std::find(result.begin(), result.end(), candidate) == result.end()) {
                result.push_back(candidate);
            }
            ++it;
            ++attempts;
        }

        return result;
    }

    size_t NodeCount() const { return physical_nodes_.size(); }
    size_t VirtualPointCount() const { return ring_.size(); }

private:
    int virtual_nodes_per_node_;
    std::map<uint64_t, std::string> ring_;
    std::set<std::string> physical_nodes_;
};