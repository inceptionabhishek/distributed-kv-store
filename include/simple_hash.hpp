#pragma once

#include <cstdint>
#include <string>

// FNV-1a hash: simple, fast, and -- critically for our router --
// deterministic across every process run, every machine, forever.
// (Unlike std::hash<std::string>, which some stdlib implementations
// randomize per-process for hash-flood DoS resistance.)
inline uint64_t fnv1a_hash(const std::string& input) {
    uint64_t hash = 14695981039346656037ULL;   // FNV offset basis
    const uint64_t prime = 1099511628211ULL;   // FNV prime

    for (char c : input) {
        hash ^= static_cast<uint64_t>(static_cast<unsigned char>(c));
        hash *= prime;
    }
    return hash;
}