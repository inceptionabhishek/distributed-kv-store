#include <gtest/gtest.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include "consistent_hash_ring.hpp"

// --- Edge cases ---

TEST(ConsistentHashRingTest, EmptyRingReturnsEmptyString) {
    ConsistentHashRing ring(10);
    EXPECT_EQ(ring.GetNodeForKey("any_key"), "");
}

TEST(ConsistentHashRingTest, SingleNodeOwnsEveryKey) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(ring.GetNodeForKey("key" + std::to_string(i)), "node0");
    }
}

TEST(ConsistentHashRingTest, RemovingOnlyNodeMakesRingEmptyAgain) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    ring.RemoveNode("node0");
    EXPECT_EQ(ring.GetNodeForKey("any_key"), "");
    EXPECT_EQ(ring.NodeCount(), 0u);
}

// --- Determinism: the property the whole design depends on ---

TEST(ConsistentHashRingTest, SameKeyAlwaysMapsToSameNode) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.AddNode("node2");

    std::string first_answer = ring.GetNodeForKey("stable_key");
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(ring.GetNodeForKey("stable_key"), first_answer);
    }
}

// --- Bookkeeping sanity ---

TEST(ConsistentHashRingTest, VirtualPointCountMatchesNodesTimesVirtualNodes) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.AddNode("node2");
    EXPECT_EQ(ring.VirtualPointCount(), 30u);
    EXPECT_EQ(ring.NodeCount(), 3u);
}

TEST(ConsistentHashRingTest, RemoveNodeDropsItsVirtualPoints) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.RemoveNode("node0");
    EXPECT_EQ(ring.VirtualPointCount(), 10u);
    EXPECT_EQ(ring.NodeCount(), 1u);
}

// --- The actual point of consistent hashing: minimal movement ---

TEST(ConsistentHashRingTest, AddingNodeMovesOnlyAFractionOfKeys) {
    const int num_keys = 10000;
    std::vector<std::string> keys;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back("key" + std::to_string(i));
    }

    ConsistentHashRing ring(100);   // more virtual points = smoother distribution
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.AddNode("node2");

    std::unordered_map<std::string, std::string> before;
    for (const auto& key : keys) {
        before[key] = ring.GetNodeForKey(key);
    }

    ring.AddNode("node3");

    int moved = 0;
    for (const auto& key : keys) {
        if (ring.GetNodeForKey(key) != before[key]) moved++;
    }

    double moved_fraction = static_cast<double>(moved) / num_keys;

    EXPECT_LT(moved_fraction, 0.40)
        << "Too many keys moved (" << (moved_fraction * 100) << "%) -- "
        << "this suggests virtual points aren't well distributed.";
}

TEST(ConsistentHashRingTest, KeysOnlyMoveToTheNewNodeNeverBetweenOldNodes) {
    // A stronger, more specific correctness property than the fraction
    // check above: when adding one new node, every key that moves must
    // move specifically TO the new node. If any key moved from node0 to
    // node1 (an old node to another old node), that's a bug -- consistent
    // hashing guarantees old nodes never steal keys from each other.
    const int num_keys = 5000;
    std::vector<std::string> keys;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back("key" + std::to_string(i));
    }

    ConsistentHashRing ring(50);
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.AddNode("node2");

    std::unordered_map<std::string, std::string> before;
    for (const auto& key : keys) {
        before[key] = ring.GetNodeForKey(key);
    }

    ring.AddNode("node3");

    for (const auto& key : keys) {
        std::string after = ring.GetNodeForKey(key);
        if (after != before[key]) {
            EXPECT_EQ(after, "node3")
                << "Key '" << key << "' moved from " << before[key]
                << " to " << after << " instead of staying or moving to the new node";
        }
    }
}

// --- Load balance sanity (not perfectly even, but not wildly skewed) ---

TEST(ConsistentHashRingTest, LoadIsReasonablyBalancedAcrossNodes) {
    const int num_keys = 10000;
    ConsistentHashRing ring(100);
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.AddNode("node2");

    std::unordered_map<std::string, int> counts;
    for (int i = 0; i < num_keys; ++i) {
        std::string owner = ring.GetNodeForKey("key" + std::to_string(i));
        counts[owner]++;
    }

    ASSERT_EQ(counts.size(), 3u);
    for (const auto& [node, count] : counts) {
        double fraction = static_cast<double>(count) / num_keys;
        EXPECT_GT(fraction, 0.15) << node << " got too few keys";
        EXPECT_LT(fraction, 0.55) << node << " got too many keys";
    }
}



// --- GetNodesForKey: replication support ---

TEST(ConsistentHashRingTest, GetNodesForKeyReturnsDistinctNodes) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.AddNode("node2");

    auto nodes = ring.GetNodesForKey("some_key", 3);
    ASSERT_EQ(nodes.size(), 3u);

    std::unordered_set<std::string> distinct(nodes.begin(), nodes.end());
    EXPECT_EQ(distinct.size(), 3u) << "GetNodesForKey returned duplicate nodes";
}

TEST(ConsistentHashRingTest, GetNodesForKeyFirstEntryMatchesGetNodeForKey) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    ring.AddNode("node1");
    ring.AddNode("node2");

    std::string primary = ring.GetNodeForKey("some_key");
    auto nodes = ring.GetNodesForKey("some_key", 3);
    ASSERT_FALSE(nodes.empty());
    EXPECT_EQ(nodes[0], primary);
}

TEST(ConsistentHashRingTest, GetNodesForKeyCapsAtAvailableNodeCount) {
    ConsistentHashRing ring(10);
    ring.AddNode("node0");
    ring.AddNode("node1");

    auto nodes = ring.GetNodesForKey("some_key", 3);
    EXPECT_EQ(nodes.size(), 2u);
}

TEST(ConsistentHashRingTest, GetNodesForKeyOnEmptyRingReturnsEmpty) {
    ConsistentHashRing ring(10);
    auto nodes = ring.GetNodesForKey("some_key", 3);
    EXPECT_TRUE(nodes.empty());
}