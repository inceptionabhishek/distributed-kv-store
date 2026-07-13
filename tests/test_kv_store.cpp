#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include "kv_store.hpp"

TEST(KVStoreTest, GetOnEmptyStoreReturnsNullopt) {
    KVStore store;
    EXPECT_EQ(store.get("missing"), std::nullopt);
}

TEST(KVStoreTest, PutThenGetReturnsValue) {
    KVStore store;
    store.put("key1", "value1", 100);
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "value1");
    EXPECT_EQ(result->timestamp, 100u);
}

TEST(KVStoreTest, NewerTimestampOverwritesOlder) {
    KVStore store;
    store.put("key1", "first", 100);
    store.put("key1", "second", 200);
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "second");
    EXPECT_EQ(result->timestamp, 200u);
}

TEST(KVStoreTest, OlderTimestampArrivingLateIsIgnored) {
    KVStore store;
    store.put("key1", "newer_value", 200);
    store.put("key1", "stale_late_arrival", 100);
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "newer_value");
    EXPECT_EQ(result->timestamp, 200u);
}

TEST(KVStoreTest, EqualTimestampOverwrites) {
    KVStore store;
    store.put("key1", "first", 100);
    store.put("key1", "second", 100);
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "second");
}

TEST(KVStoreTest, PutEmptyStringValueIsDistinctFromMissing) {
    KVStore store;
    store.put("key1", "", 100);
    auto result = store.get("key1");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->value, "");
}

TEST(KVStoreTest, RemoveExistingKeyReturnsTrue) {
    KVStore store;
    store.put("key1", "value1", 100);
    EXPECT_TRUE(store.remove("key1"));
    EXPECT_EQ(store.get("key1"), std::nullopt);
}

TEST(KVStoreTest, RemoveMissingKeyReturnsFalse) {
    KVStore store;
    EXPECT_FALSE(store.remove("never_existed"));
}

TEST(KVStoreTest, RemoveIsIdempotent) {
    KVStore store;
    store.put("key1", "value1", 100);
    EXPECT_TRUE(store.remove("key1"));
    EXPECT_FALSE(store.remove("key1"));
}

TEST(KVStoreTest, SizeReflectsPutsAndRemoves) {
    KVStore store;
    EXPECT_EQ(store.size(), 0u);
    store.put("a", "1", 100);
    store.put("b", "2", 100);
    EXPECT_EQ(store.size(), 2u);
    store.remove("a");
    EXPECT_EQ(store.size(), 1u);
}

TEST(KVStoreTest, OverwritingDoesNotChangeSize) {
    KVStore store;
    store.put("a", "1", 100);
    store.put("a", "2", 200);
    EXPECT_EQ(store.size(), 1u);
}

TEST(KVStoreTest, ConcurrentPutsAllSucceedWithNoLostWrites) {
    KVStore store;
    const int num_threads = 8;
    const int puts_per_thread = 5000;

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&store, t, puts_per_thread]() {
            for (int i = 0; i < puts_per_thread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                store.put(key, "v" + std::to_string(i), static_cast<uint64_t>(i));
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(store.size(), static_cast<size_t>(num_threads) * puts_per_thread);
}

TEST(KVStoreTest, ConcurrentReadsAndWritesOnSameKeyDoNotCrash) {
    KVStore store;
    store.put("shared_key", "initial", 0);

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&store, t]() {
            for (int i = 0; i < 1000; ++i) {
                store.put("shared_key", "writer" + std::to_string(t), static_cast<uint64_t>(i));
                (void)store.get("shared_key");
            }
        });
    }
    for (auto& th : threads) th.join();

    auto final_val = store.get("shared_key");
    EXPECT_TRUE(final_val.has_value());
}