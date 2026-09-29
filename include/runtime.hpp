#pragma once
#include "kv_store.hpp"
#include <cstdlib>
#include <map>
#include <sstream>
#include <stdexcept>
#include <vector>
inline std::vector<std::string> SplitNodes(const std::string& text) {
    std::vector<std::string> nodes; std::stringstream stream(text); std::string node;
    while (std::getline(stream, node, ',')) { if (!node.empty()) nodes.push_back(node); }
    if (nodes.empty()) throw std::invalid_argument("empty membership"); return nodes;
}
inline std::string DefaultNodes() {
    const char* value = std::getenv("KV_NODES");
    return value ? value : "localhost:50051,localhost:50052,localhost:50053,localhost:50054";
}
struct Arguments {
    std::map<std::string, std::string> flags;
    std::vector<std::string> positional;
    Arguments(int argc, char** argv) {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg.rfind("--", 0) == 0) {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + arg);
                flags[arg.substr(2)] = argv[++i];
            } else positional.push_back(arg);
        }
    }
    std::string Get(const std::string& key, const std::string& fallback) const {
        auto it = flags.find(key); return it == flags.end() ? fallback : it->second;
    }
    int Int(const std::string& key, int fallback) const {
        auto text = Get(key, std::to_string(fallback)); size_t parsed = 0;
        int n = std::stoi(text, &parsed);
        if (parsed != text.size()) throw std::invalid_argument("invalid integer: " + key); return n;
    }
};
inline Durability ParseDurability(const std::string& text) {
    if (text == "always") return Durability::Always;
    if (text == "periodic") return Durability::Periodic;
    if (text == "memory") return Durability::Memory;
    throw std::invalid_argument("durability must be always, periodic, or memory");
}
