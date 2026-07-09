#pragma once

#include <map>
#include <string>
#include <vector>
#include <set>

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

    // Returns which physical node owns this key.
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

    size_t NodeCount() const { return physical_nodes_.size(); }
    size_t VirtualPointCount() const { return ring_.size(); }

private:
    int virtual_nodes_per_node_;
    std::map<uint64_t, std::string> ring_;
    std::set<std::string> physical_nodes_;
};