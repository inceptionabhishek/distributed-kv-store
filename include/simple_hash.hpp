#pragma once

#include <cstdint>
#include <string>

inline uint64_t fnv1a_hash(const std::string& input) {
    uint64_t hash = 14695981039346656037ULL;   // FNV offset basis
    const uint64_t prime = 1099511628211ULL;   // FNV prime

    for (char c : input) {
        hash ^= static_cast<uint64_t>(static_cast<unsigned char>(c));
        hash *= prime;
    }
    return hash;
}

// FNV-1a's raw output has weak avalanche: inputs that differ only in
// their last character or two (like "node0#0" vs "node0#1") produce
// outputs that are still numerically close together. That's harmless
// for hash(key) % N, but fatal for a hash ring, since virtual points
// need to scatter across the full 64-bit space, not cluster.
// This finalizer (borrowed from MurmurHash3) fixes that by thoroughly
// mixing every bit of the input into every bit of the output.
inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

// Use this for anything that needs points scattered across the ring
// (virtual node placement, key lookup on the ring).
inline uint64_t ring_hash(const std::string& input) {
    return mix64(fnv1a_hash(input));
}