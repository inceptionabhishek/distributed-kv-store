#include <gtest/gtest.h>
#include "gateway_service.hpp"
#include "node_service.hpp"
#include <filesystem>
#include <csignal>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>

namespace {
struct TempDir {
    std::string path;
    TempDir() { char name[] = "/tmp/kv-cluster-XXXXXX"; auto p = ::mkdtemp(name); if (!p) throw std::runtime_error("mkdtemp"); path = p; }
    ~TempDir() { std::error_code error; std::filesystem::remove_all(path, error); }
};
struct Node {
    NodeService service;
    std::unique_ptr<grpc::Server> server;
    std::string address;
    Node() { Start(); }
    void Start() {
        grpc::ServerBuilder builder; int port = 0;
        builder.AddListeningPort(address.empty() ? "127.0.0.1:0" : address, grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service); server = builder.BuildAndStart();
        if (!server) throw std::runtime_error("test server bind failed");
        address = "127.0.0.1:" + std::to_string(port);
    }
    void Stop() { if (server) { server->Shutdown(); server.reset(); } }
    ~Node() { Stop(); }
};
bool Eventually(const std::function<bool()>& predicate, int timeout_ms = 4000) {
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    do { if (predicate()) return true; std::this_thread::sleep_for(std::chrono::milliseconds(20)); }
    while (std::chrono::steady_clock::now() < end); return predicate();
}
RouterOptions Options() {
    RouterOptions o; o.operation_timeout = std::chrono::milliseconds(250);
    o.replica_timeout = std::chrono::milliseconds(150); o.repair_interval = std::chrono::milliseconds(0);
    return o;
}
Entry Record(std::string value, uint64_t time, bool deleted = false) {
    return {"key", {value, time, {time, 0, "test"}, deleted}};
}
class ClusterTest : public ::testing::Test {
protected:
    Node a, b, c;
    std::vector<std::string> nodes() { return {a.address, b.address, c.address}; }
};
}
TEST_F(ClusterTest, QuorumReadWriteAndMissingAreDistinct) {
    Router router(nodes(), Options()); EXPECT_TRUE(router.Put("key", "value"));
    auto read = router.Read("key"); ASSERT_EQ(read.status, ReadStatus::Found); EXPECT_EQ(read.data->value, "value");
    EXPECT_EQ(router.Read("missing").status, ReadStatus::NotFound);
}
TEST_F(ClusterTest, OneUnavailableReplicaStillAllowsReadsAndWrites) {
    c.service.unavailable = true; Router router(nodes(), Options());
    EXPECT_TRUE(router.Put("key", "value")); EXPECT_EQ(router.Read("key").status, ReadStatus::Found);
    router.Drain(); EXPECT_GT(router.PendingHints(), 0u);
}
TEST_F(ClusterTest, TwoUnavailableReplicasFailQuorumWithinDeadline) {
    b.service.unavailable = true; c.service.unavailable = true; Router router(nodes(), Options());
    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(router.Put("key", "value")); EXPECT_EQ(router.Read("key").status, ReadStatus::Unavailable);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
}
TEST_F(ClusterTest, QuorumReturnsBeforeSlowThirdReplica) {
    Router router(nodes(), Options()); c.service.delay_ms = 1000;
    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(router.Put("key", "value"));
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(140));
    EXPECT_EQ(router.Read("key").status, ReadStatus::Found);
}
TEST_F(ClusterTest, SlowQuorumIsBoundedAndTimeoutsAreMeasured) {
    Router router(nodes(), Options()); a.service.delay_ms = 1000; b.service.delay_ms = 1000; c.service.delay_ms = 1000;
    auto start = std::chrono::steady_clock::now(); EXPECT_FALSE(router.Put("key", "value"));
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(600));
    EXPECT_NE(router.MetricsText().find("kv_deadline_exceeded_total"), std::string::npos);
}
TEST_F(ClusterTest, AutomaticReplayRepairsRecoveredReplica) {
    c.service.unavailable = true; Router router(nodes(), Options());
    EXPECT_TRUE(router.Put("key", "value")); router.Drain();
    c.service.unavailable = false;
    ASSERT_TRUE(Eventually([&] { return c.service.store().get("key").has_value(); }));
    EXPECT_EQ(c.service.store().get("key")->value, "value");
    EXPECT_TRUE(Eventually([&] { return router.PendingHints() == 0; }));
}
TEST_F(ClusterTest, HintSurvivesCoordinatorRestartAndThenReplays) {
    TempDir d; auto o = Options(); o.state_directory = d.path;
    c.service.unavailable = true;
    { Router router(nodes(), o); EXPECT_TRUE(router.Put("key", "value")); router.Drain(); EXPECT_GT(router.PendingHints(), 0u); }
    c.service.unavailable = false;
    { Router router(nodes(), o); ASSERT_TRUE(Eventually([&] { return c.service.store().get("key").has_value(); })); }
}
TEST_F(ClusterTest, ReadRepairUpdatesStaleReplica) {
    a.service.store().apply(Record("new", 200)); b.service.store().apply(Record("new", 200));
    c.service.store().apply(Record("old", 100)); Router router(nodes(), Options());
    EXPECT_EQ(router.Read("key").data->value, "new");
    ASSERT_TRUE(Eventually([&] { auto v = c.service.store().get("key"); return v && v->value == "new"; }));
}
TEST_F(ClusterTest, TombstoneRepairsOldReplicaAndPreventsResurrection) {
    Router router(nodes(), Options()); EXPECT_TRUE(router.Put("key", "old")); router.Drain();
    c.service.unavailable = true; EXPECT_TRUE(router.Remove("key")); router.Drain();
    EXPECT_EQ(router.Read("key").status, ReadStatus::NotFound);
    c.service.unavailable = false;
    ASSERT_TRUE(Eventually([&] { auto v = c.service.store().raw_get("key"); return v && v->tombstone; }));
    EXPECT_FALSE(c.service.store().get("key"));
}
TEST_F(ClusterTest, StaleWritesDoNotCountAsAcknowledgements) {
    for (auto node : {&a, &b, &c}) node->service.store().apply(Record("new", 200));
    Router router(nodes(), Options()); auto result = router.Mutate(Record("old", 100));
    EXPECT_FALSE(result.success); EXPECT_EQ(result.status, kvstore::STALE);
}
TEST_F(ClusterTest, RetryingSameMutationIsSafeButPayloadReuseFails) {
    Router router(nodes(), Options()); auto e = router.NewEntry("key", "value");
    EXPECT_TRUE(router.Mutate(e).success); router.Drain();
    auto retry = router.Mutate(e); EXPECT_TRUE(retry.success); EXPECT_EQ(retry.status, kvstore::DUPLICATE);
    e.data.value = "different"; router.Drain(); EXPECT_FALSE(router.Mutate(e).success);
}
TEST_F(ClusterTest, AntiEntropyRepairsKeysNeverReadByClient) {
    a.service.store().apply(Record("only-copy", 300)); Router router(nodes(), Options());
    EXPECT_TRUE(router.RepairOnce());
    ASSERT_TRUE(b.service.store().get("key")); EXPECT_EQ(b.service.store().get("key")->value, "only-copy");
    ASSERT_TRUE(c.service.store().get("key"));
}
TEST_F(ClusterTest, ConcurrentMutationsConvergeByVersion) {
    Router router(nodes(), Options());
    std::vector<std::thread> workers;
    for (int i = 0; i < 12; ++i) workers.emplace_back([&, i] {
        router.Mutate(Record("v" + std::to_string(i), 100 + i));
    });
    for (auto& worker : workers) worker.join(); router.Drain(); EXPECT_TRUE(router.RepairOnce());
    EXPECT_EQ(a.service.store().get("key")->value, "v11");
    EXPECT_EQ(b.service.store().get("key")->value, "v11");
    EXPECT_EQ(c.service.store().get("key")->value, "v11");
}
TEST_F(ClusterTest, MembershipMigrationPreservesValuesAndTombstonesAcrossRestart) {
    TempDir directory; Node d; auto o = Options(); o.state_directory = directory.path;
    auto expanded = nodes(); expanded.push_back(d.address);
    {
        Router router(nodes(), o);
        for (int i = 0; i < 30; ++i) ASSERT_TRUE(router.Put("key" + std::to_string(i), "value"));
        ASSERT_TRUE(router.Remove("deleted")); router.Drain();
        std::string error; ASSERT_TRUE(router.Reconfigure(expanded, 2, &error)) << error;
        EXPECT_EQ(router.Epoch(), 2u);
        for (int i = 0; i < 30; ++i) {
            auto key = "key" + std::to_string(i);
            ASSERT_EQ(router.Read(key).status, ReadStatus::Found);
            for (const auto& replica : router.ReplicaAddressesFor(key)) {
                Node* node = replica == a.address ? &a : replica == b.address ? &b : replica == c.address ? &c : &d;
                EXPECT_TRUE(node->service.store().get(key));
            }
        }
        EXPECT_EQ(router.Read("deleted").status, ReadStatus::NotFound);
    }
    Router restored(nodes(), o); EXPECT_EQ(restored.Epoch(), 2u);
    EXPECT_EQ(restored.Read("key1").status, ReadStatus::Found);
}
TEST_F(ClusterTest, UnsafeMigrationLeavesOldRingActive) {
    Router router(nodes(), Options()); Node d; auto expanded = nodes(); expanded.push_back(d.address);
    c.service.unavailable = true; std::string error;
    EXPECT_FALSE(router.Reconfigure(expanded, 2, &error)); EXPECT_EQ(router.Epoch(), 1u);
    EXPECT_FALSE(router.Reconfigure(nodes(), 1, &error));
}
TEST_F(ClusterTest, GracefulDecommissionPreservesData) {
    Node d; auto members = nodes(); members.push_back(d.address); Router router(members, Options());
    EXPECT_TRUE(router.Put("key", "value")); router.Drain(); std::string error;
    EXPECT_TRUE(router.Reconfigure(nodes(), 2, &error)) << error; d.Stop();
    EXPECT_EQ(router.Read("key").status, ReadStatus::Found);
}
TEST_F(ClusterTest, ActualNetworkEndpointCanStopAndRestart) {
    Router router(nodes(), Options()); c.Stop();
    EXPECT_TRUE(router.Put("key", "value")); router.Drain(); c.Start();
    ASSERT_TRUE(Eventually([&] { return c.service.store().get("key").has_value(); }));
}
TEST_F(ClusterTest, InjectedRequestDropsRecoverThroughHints) {
    Router router(nodes(), Options()); c.service.fail_every = 1;
    EXPECT_TRUE(router.Put("key", "value")); router.Drain(); EXPECT_GT(router.PendingHints(), 0u);
    c.service.fail_every = 0;
    EXPECT_TRUE(Eventually([&] { auto value = c.service.store().get("key"); return value && value->value == "value"; }));
}
TEST_F(ClusterTest, AutomaticAntiEntropyRepairsUnreadKeys) {
    auto o = Options(); o.repair_interval = std::chrono::milliseconds(100);
    Router router(nodes(), o); a.service.store().apply(Record("unread", 100));
    EXPECT_TRUE(Eventually([&] { return b.service.store().get("key").has_value() && c.service.store().get("key").has_value(); }));
}
TEST_F(ClusterTest, ParentDeadlineBoundsReplicaCalls) {
    Router router(nodes(), Options());
    a.service.delay_ms = 1000; b.service.delay_ms = 1000; c.service.delay_ms = 1000;
    auto start = std::chrono::steady_clock::now();
    auto result = router.Read("key", std::chrono::system_clock::now() + std::chrono::milliseconds(30));
    EXPECT_EQ(result.status, ReadStatus::Unavailable);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(140));
}
TEST_F(ClusterTest, CoordinatorClockReservationSurvivesRestart) {
    TempDir directory; auto o = Options(); o.state_directory = directory.path; Version prior;
    { Router router(nodes(), o); prior = router.NewVersion(); }
    Router restored(nodes(), o); EXPECT_TRUE(prior < restored.NewVersion());
}
TEST(PoolTest, BoundedQueueRejectsOverloadWithoutDetachedThreads) {
    ThreadPool pool(1, 1); std::promise<void> started, release;
    auto wait = release.get_future().share();
    ASSERT_TRUE(pool.Submit([&] { started.set_value(); wait.wait(); }));
    started.get_future().wait(); EXPECT_TRUE(pool.Submit([] {})); EXPECT_FALSE(pool.Submit([] {}));
    release.set_value(); pool.Drain(); EXPECT_TRUE(pool.Submit([] {}));
}
TEST(RouterConfigurationTest, InvalidQuorumAndDuplicateNodesAreRejected) {
    auto o = Options(); o.read_quorum = 1;
    EXPECT_THROW(Router({"localhost:1", "localhost:2", "localhost:3"}, o), std::invalid_argument);
    o = Options(); EXPECT_THROW(Router({"localhost:1", "localhost:1", "localhost:3"}, o), std::invalid_argument);
}
TEST_F(ClusterTest, GatewayExposesQuorumAndAdminAPI) {
    Router router(nodes(), Options()); GatewayService service(router);
    grpc::ServerBuilder builder; int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service); auto server = builder.BuildAndStart(); ASSERT_TRUE(server);
    auto stub = kvstore::KVStoreService::NewStub(grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
    grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    auto request = PutRequestFor(router.NewEntry("key", "value")); kvstore::PutResponse response;
    ASSERT_TRUE(stub->Put(&context, request, &response).ok()); EXPECT_TRUE(response.success());
    server->Shutdown();
}
TEST(ProcessTest, AcknowledgedWALWriteSurvivesSIGKILL) {
    TempDir d;
    int fd = ::socket(AF_INET, SOCK_STREAM, 0); ASSERT_GE(fd, 0);
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t size = sizeof(addr); ASSERT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &size), 0);
    int port = ntohs(addr.sin_port); ::close(fd);
    std::string address = "127.0.0.1:" + std::to_string(port), port_text = std::to_string(port);
    auto start = [&]() {
        pid_t pid = ::fork();
        if (pid == 0) {
            int output = ::open("/dev/null", O_WRONLY); ::dup2(output, STDOUT_FILENO); ::dup2(output, STDERR_FILENO); ::close(output);
            ::execl(KV_SERVER_PATH, KV_SERVER_PATH, "--port", port_text.c_str(), "--data", d.path.c_str(), "--durability", "always", nullptr);
            ::_exit(127);
        }
        return pid;
    };
    struct Child {
        pid_t pid;
        ~Child() { if (pid > 0) { ::kill(pid, SIGKILL); ::waitpid(pid, nullptr, 0); } }
        void Kill() { ::kill(pid, SIGKILL); ::waitpid(pid, nullptr, 0); pid = -1; }
    } child{start()};
    ASSERT_GT(child.pid, 0);
    auto stub = kvstore::KVStoreService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    auto ping = [&] {
        grpc::ClientContext c; c.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
        kvstore::PingRequest r; kvstore::PingResponse response; return stub->Ping(&c, r, &response).ok();
    };
    ASSERT_TRUE(Eventually(ping, 5000));
    grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    auto request = PutRequestFor(Record("durable", 100)); kvstore::PutResponse response;
    ASSERT_TRUE(stub->Put(&context, request, &response).ok()); ASSERT_TRUE(response.success());
    child.Kill(); child.pid = start(); ASSERT_TRUE(Eventually(ping, 5000));
    grpc::ClientContext get_context; get_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    kvstore::GetRequest get; get.set_key("key"); kvstore::GetResponse read;
    ASSERT_TRUE(stub->Get(&get_context, get, &read).ok()); EXPECT_TRUE(read.found()); EXPECT_EQ(read.value(), "durable");
}
