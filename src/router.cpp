#include "gateway_service.hpp"
#include "metrics_http.hpp"
#include "runtime.hpp"
#include "shutdown.hpp"
#include <iostream>
int main(int argc, char** argv) {
    try {
        BlockShutdownSignals();
        Arguments args(argc, argv);
        RouterOptions options;
        options.state_directory = args.Get("data", ".data/router");
        options.verbose = args.Int("verbose", 1) != 0;
        options.replication_factor = args.Int("replicas", 3);
        options.write_quorum = args.Int("write-quorum", 2); options.read_quorum = args.Int("read-quorum", 2);
        options.operation_timeout = std::chrono::milliseconds(args.Int("timeout-ms", 750));
        options.replica_timeout = std::chrono::milliseconds(args.Int("replica-timeout-ms", 500));
        options.repair_interval = std::chrono::milliseconds(args.Int("repair-ms", 5000));
        Router router(SplitNodes(args.Get("nodes", DefaultNodes())), options);
        if (!args.flags.count("serve")) {
            bool ok = router.Put("user:1", "abhishek");
            std::string value; ok = router.Get("user:1", &value) && value == "abhishek" && ok;
            ok = router.Remove("user:1") && ok;
            auto read = router.Read("user:1"); ok = read.status == ReadStatus::NotFound && ok;
            router.Drain();
            std::cout << (ok ? "PASS" : "FAIL") << "\n"; return ok ? 0 : 1;
        }
        int port = args.Int("serve", 50050);
        if (port < 1 || port > 65535) throw std::invalid_argument("invalid port");
        GatewayService service(router);
        MetricsHttp metrics(args.Int("metrics-port", 9090), [&] { return router.MetricsText(); });
        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:" + std::to_string(port), grpc::InsecureServerCredentials());
        builder.RegisterService(&service); auto server = builder.BuildAndStart();
        if (!server) throw std::runtime_error("failed to bind coordinator port");
        std::cout << "Coordinator listening on :" << port << " epoch=" << router.Epoch() << std::endl;
        WaitWithSignals(server.get());
    } catch (const std::exception& e) { std::cerr << e.what() << "\n"; return 1; }
}
