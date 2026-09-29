#include "node_service.hpp"
#include "metrics_http.hpp"
#include "runtime.hpp"
#include "shutdown.hpp"
#include <iostream>
int main(int argc, char** argv) {
    try {
        BlockShutdownSignals();
        Arguments args(argc, argv);
        int port = args.Int("port", args.positional.empty() ? 50051 : std::stoi(args.positional[0]));
        if (port < 1 || port > 65535) throw std::invalid_argument("invalid port");
        auto durability = ParseDurability(args.Get("durability", "always"));
        int snapshots = args.Int("snapshot-every", 10000), fail_every = args.Int("fail-every", 0);
        if (snapshots < 0 || fail_every < 0) throw std::invalid_argument("negative snapshot/fault setting");
        NodeService service(args.Get("data", ".data/node-" + std::to_string(port)), durability, snapshots);
        service.delay_ms = args.Int("delay-ms", 0); service.fail_every = fail_every;
        if (service.delay_ms < 0) throw std::invalid_argument("negative delay");
        MetricsHttp metrics(args.Int("metrics-port", 0), [&] { return service.MetricsText(); });
        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:" + std::to_string(port), grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        auto server = builder.BuildAndStart();
        if (!server) throw std::runtime_error("failed to bind storage port");
        std::cout << "Storage node listening on :" << port << " durability=" << args.Get("durability", "always") << std::endl;
        WaitWithSignals(server.get());
    } catch (const std::exception& e) { std::cerr << e.what() << "\n"; return 1; }
}
