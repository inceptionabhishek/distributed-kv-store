#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include "kv_store.hpp"

// --- Basic put/get behavior ---

TEST(KVStoreTest, GetOnEmptyStoreReturnsNullopt) {
    KVStore store;
    EXPECT_EQ(store.get("missing"), std::nullopt);
}

TEST(KVStoreTest, PutThenGetReturnsValue) {
    KVStore store;
    store.put("key1", "value1");
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value1");
}

TEST(KVStoreTest, PutOverwritesExistingKey) {
    KVStore store;
    store.put("key1", "first");
    store.put("key1", "second");
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "second");
}

TEST(KVStoreTest, PutEmptyStringValueIsDistinctFromMissing) {
    // Edge case: an empty string is a valid value, not the same as "not found".
    // This is exactly why we use std::optional instead of "" as a sentinel.
    KVStore store;
    store.put("key1", "");
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "");
}

// --- remove() ---

TEST(KVStoreTest, RemoveExistingKeyReturnsTrue) {
    KVStore store;
    store.put("key1", "value1");
    EXPECT_TRUE(store.remove("key1"));
    EXPECT_EQ(store.get("key1"), std::nullopt);
}

TEST(KVStoreTest, RemoveMissingKeyReturnsFalse) {
    KVStore store;
    EXPECT_FALSE(store.remove("never_existed"));
}

TEST(KVStoreTest, RemoveIsIdempotent) {
    // Removing the same key twice shouldn't throw or corrupt state --
    // the second remove is just a no-op that reports false.
    KVStore store;
    store.put("key1", "value1");
    EXPECT_TRUE(store.remove("key1"));
    EXPECT_FALSE(store.remove("key1"));
}

// --- size() ---

TEST(KVStoreTest, SizeReflectsPutsAndRemoves) {
    KVStore store;
    EXPECT_EQ(store.size(), 0u);
    store.put("a", "1");
    store.put("b", "2");
    EXPECT_EQ(store.size(), 2u);
    store.remove("a");
    EXPECT_EQ(store.size(), 1u);
}

TEST(KVStoreTest, OverwritingDoesNotChangeSize) {
    KVStore store;
    store.put("a", "1");
    store.put("a", "2");   // same key, different value
    EXPECT_EQ(store.size(), 1u);
}

// --- Concurrency: the actual reason the mutex exists ---

TEST(KVStoreTest, ConcurrentPutsAllSucceedWithNoLostWrites) {
    KVStore store;
    const int num_threads = 8;
    const int puts_per_thread = 5000;

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&store, t, puts_per_thread]() {
            for (int i = 0; i < puts_per_thread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                store.put(key, "v" + std::to_string(i));
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(store.size(), static_cast<size_t>(num_threads) * puts_per_thread);
}

TEST(KVStoreTest, ConcurrentReadsAndWritesOnSameKeyDoNotCrash) {
    // This test's assertion is weak (just checks it eventually holds one
    // of the written values) -- the real point is that it must not crash
    // or hang under TSan/ASan. Run with sanitizers occasionally to be sure.
    KVStore store;
    store.put("shared_key", "initial");

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&store, t]() {
            for (int i = 0; i < 1000; ++i) {
                store.put("shared_key", "writer" + std::to_string(t));
                (void)store.get("shared_key");
            }
        });
    }
    for (auto& th : threads) th.join();

    auto final_val = store.get("shared_key");
    EXPECT_TRUE(final_val.has_value());
}