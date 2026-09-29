#include <gtest/gtest.h>
#include "hint_queue.hpp"
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() { char name[] = "/tmp/kv-test-XXXXXX"; auto p = ::mkdtemp(name); if (!p) throw std::runtime_error("mkdtemp"); path = p; }
    ~TempDir() { std::error_code error; std::filesystem::remove_all(path, error); }
};
Entry Value(std::string key, std::string value, uint64_t time, std::string writer = "a", bool deleted = false) {
    return {key, {value, time, {time, 0, writer}, deleted}};
}
}
TEST(VersionTest, WriterBreaksTiesDeterministically) {
    auto a = Value("k", "a", 100, "a"), b = Value("k", "b", 100, "b");
    KVStore first, second;
    first.apply(a); first.apply(b); second.apply(b); second.apply(a);
    EXPECT_EQ(first.get("k")->value, "b"); EXPECT_EQ(second.get("k")->value, "b");
}
TEST(VersionTest, ClockObservesRemoteVersionsAndAdvances) {
    HybridClock clock("a");
    Version remote{WallMillis() + 100000, 17, "b"};
    clock.Observe(remote); auto next = clock.Next();
    EXPECT_TRUE(remote < next); EXPECT_TRUE(next < clock.Next());
}
TEST(StorageTest, DuplicateIsIdempotentAndChangedPayloadIsRejected) {
    KVStore store; auto e = Value("k", "v", 100);
    EXPECT_EQ(store.apply(e), ApplyResult::Applied); EXPECT_EQ(store.apply(e), ApplyResult::Duplicate);
    e.data.value = "different"; EXPECT_EQ(store.apply(e), ApplyResult::Conflict);
    EXPECT_EQ(store.get("k")->value, "v");
}
TEST(StorageTest, TombstonePreventsOldValueResurrection) {
    KVStore store; store.apply(Value("k", "old", 100));
    store.apply(Value("k", "", 200, "a", true));
    EXPECT_EQ(store.apply(Value("k", "old", 100)), ApplyResult::Stale);
    EXPECT_FALSE(store.get("k")); ASSERT_TRUE(store.raw_get("k")); EXPECT_TRUE(store.raw_get("k")->tombstone);
}
TEST(StorageTest, WALAndSnapshotsSurviveRestartIncludingDeletes) {
    TempDir d;
    { KVStore store(d.path, Durability::Always, 2);
      store.apply(Value("a", "one", 100)); store.apply(Value("b", "two", 100));
      store.apply(Value("a", "", 200, "a", true)); }
    { KVStore store(d.path, Durability::Always);
      EXPECT_FALSE(store.get("a")); ASSERT_TRUE(store.raw_get("a")); EXPECT_TRUE(store.raw_get("a")->tombstone);
      ASSERT_TRUE(store.get("b")); EXPECT_EQ(store.get("b")->value, "two"); store.snapshot(); }
    KVStore store(d.path, Durability::Always); EXPECT_EQ(store.size(), 1u);
}
TEST(StorageTest, IncompleteWALTailIsTruncatedBeforeNewAppends) {
    TempDir d;
    { KVStore store(d.path, Durability::Always); store.apply(Value("a", "one", 100)); }
    { std::ofstream out(d.path + "/store.wal", std::ios::binary | std::ios::app); out.write("torn", 4); }
    { KVStore store(d.path, Durability::Always); EXPECT_EQ(store.get("a")->value, "one"); store.apply(Value("b", "two", 200)); }
    KVStore store(d.path, Durability::Always); EXPECT_EQ(store.get("b")->value, "two");
}
TEST(StorageTest, CorruptChecksummedRecordFailsClosed) {
    TempDir d;
    { KVStore store(d.path, Durability::Always); store.apply(Value("a", "one", 100)); }
    { std::fstream file(d.path + "/store.wal", std::ios::binary | std::ios::in | std::ios::out);
      file.seekp(16); file.put('x'); }
    EXPECT_THROW(KVStore(d.path, Durability::Always), std::runtime_error);
}
TEST(StorageTest, DataDirectoryCannotHaveTwoOwners) {
    TempDir d; KVStore store(d.path, Durability::Always);
    EXPECT_THROW(KVStore(d.path, Durability::Always), std::runtime_error);
}
TEST(StorageTest, PeriodicModeFlushesAndRecovers) {
    TempDir d;
    { KVStore store(d.path, Durability::Periodic); store.apply(Value("a", "one", 100));
      std::this_thread::sleep_for(std::chrono::milliseconds(130)); EXPECT_GT(store.sync_count(), 0u); }
    KVStore recovered(d.path, Durability::Always); EXPECT_EQ(recovered.get("a")->value, "one");
}
TEST(HintTest, CompactionAndRestartPreserveNewestMutation) {
    TempDir d;
    { HintQueue q(d.path); q.Store("node", Value("k", "old", 100)); q.Store("node", Value("k", "new", 200)); q.Snapshot(); }
    HintQueue q(d.path); ASSERT_EQ(q.size(), 1u); EXPECT_EQ(q.Due()[0].entry.data.value, "new");
}
TEST(HintTest, OldReplayCannotEraseNewerHint) {
    HintQueue q; q.Store("node", Value("k", "old", 100)); auto old = q.Due()[0];
    q.Store("node", Value("k", "new", 200)); q.Complete(old);
    ASSERT_EQ(q.size(), 1u); EXPECT_EQ(q.Due()[0].entry.data.value, "new");
}
TEST(HintTest, FailedReplayRemainsDurableAndBacksOff) {
    TempDir d;
    { HintQueue q(d.path); q.Store("node", Value("k", "v", 100)); q.Failed(q.Due()[0]);
      EXPECT_EQ(q.size(), 1u); EXPECT_TRUE(q.Due().empty()); }
    HintQueue q(d.path); EXPECT_EQ(q.size(), 1u);
}
TEST(HintTest, ExpiredHintIsRemovedDurably) {
    TempDir d;
    { HintQueue q(d.path, 1); q.Store("node", Value("k", "v", 100)); auto h = q.Due()[0];
      std::this_thread::sleep_for(std::chrono::milliseconds(5)); q.Failed(h);
      EXPECT_EQ(q.size(), 0u); EXPECT_EQ(q.expired(), 1u); }
    HintQueue q(d.path); EXPECT_EQ(q.size(), 0u);
}
