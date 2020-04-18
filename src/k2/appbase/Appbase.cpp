#include "Appbase.h"

namespace k2 {

App* ___appBase___;

int App::start(int argc, char** argv) {
    ___appBase___ = this;
    k2::VirtualNetworkStack::Dist_t vnet;
    k2::RPCProtocolFactory::Dist_t tcpproto;
    k2::RPCProtocolFactory::Dist_t rrdmaproto;
    k2::RPCProtocolFactory::Dist_t autoproto;
    k2::Prometheus prometheus;
    MultiAddressProvider addrProvider;
    RPCProtocolFactory::BuilderFunc_t tcpProtobuilder;

    addOptions()
    ("prometheus_port", bpo::value<uint16_t>()->default_value(8089), "HTTP port for the prometheus server")
    ("tcp_port", bpo::value<uint16_t>(), "If specified, this TCP port will be opened on all shards (kernel-based incoming connection load-balancing via shared bind on same port from multiple listeners. Conflicts with --tcp_endpoints")
    ("tcp_endpoints", bpo::value<std::vector<std::string>>()->multitoken(), "A list(space-delimited) of TCP listening endpoints to assign to each core. You can specify either full endpoints, e.g. 'tcp+k2rpc://192.168.1.2:12345' or just ports , e.g. '12345'. If simple ports are specified, the stack will bind to 0.0.0.0. Conflicts with --tcp_port")
    ("enable_tx_checksum", bpo::value<bool>()->default_value(false), "enables transport-level checksums (and validation) on all messages. it incurs double - read penalty(data is read separately to compute checksum)")
    //("vservers", bpo::value<std::vector<int>>()->multitoken(), "This option accepts exactly 2 integers, which specify how many virtual servers to create(1) and how many cores each server should have(2). The servers are reachable within the same process over the sim protocol, with auto-assigned names.")
    ;

    //modify some seastar::reactor default options so that it's straight-forward to write simple apps (1 core/50M memory)
    {
        auto smpopt = _app.get_options_description().find_nothrow("smp", false);
        if (smpopt) {
            boost::shared_ptr<bpo::value_semantic> semval = boost::const_pointer_cast<bpo::value_semantic>(smpopt->semantic());
            boost::shared_ptr<bpo::typed_value<unsigned>> tval = boost::dynamic_pointer_cast<bpo::typed_value<unsigned>>(semval);
            assert(tval);
            tval->default_value(1);
        }
    }
    {
        auto memopt = _app.get_options_description().find_nothrow("memory", false);
        if (memopt) {
            boost::shared_ptr<bpo::value_semantic> semval = boost::const_pointer_cast<bpo::value_semantic>(memopt->semantic());
            boost::shared_ptr<bpo::typed_value<std::string>> tval = boost::dynamic_pointer_cast<bpo::typed_value<std::string>>(semval);
            assert(tval);
            tval->default_value("50M");
        }
    }
    // we are now ready to assemble the running application
    auto result = _app.run_deprecated(argc, argv, [&] {
        auto& config = _app.configuration();
        K2INFO("Starting " << argv[0] << ", with args:");
        for (int i = 1; i < argc; i++) {
            K2INFO("\t " << argv[i]);
        }

        if (config.count("tcp_port") && config.count("tcp_endpoints")) {
            const char* msg = "Only one of tcp_port/tcp_endpoints option is allowed";
            K2ERROR(msg)
            return seastar::make_exception_future<>(std::runtime_error(msg));
        }

        if (config.count("tcp_port")) {
            auto port = config["tcp_port"].as<uint16_t>();
            tcpProtobuilder = k2::TCPRPCProtocol::builder(std::ref(vnet), port);
        } else if (config.count("tcp_endpoints")) {
            std::vector<std::string> tcp_endpoints = config["tcp_endpoints"].as<std::vector<std::string>>();
            addrProvider = MultiAddressProvider(tcp_endpoints);
            tcpProtobuilder = k2::TCPRPCProtocol::builder(std::ref(vnet), std::ref(addrProvider));
        } else {
            tcpProtobuilder = k2::TCPRPCProtocol::builder(std::ref(vnet));
        }

        // call the stop() method on each object when we're about to exit. This also deletes the objects
        seastar::engine().at_exit([&] {
            K2INFO("stop config");
            return ConfigDist().stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("stop prometheus");
            return prometheus.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("stop vnet");
            return vnet.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("stop tcpproto");
            return tcpproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("stop rrdma");
            return rrdmaproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("stop autoproto");
            return autoproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("stop dispatcher");
            return RPCDist().stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("stop user applets");
            std::vector<seastar::future<>> stopFutures;
            for (auto& stopper : _stoppers) {
                stopFutures.push_back(stopper());
            }
            return seastar::when_all(stopFutures.begin(), stopFutures.end()).discard_result();
        });

        return
            // OBJECT CREATION (via distributed<>.start())
            [&] {
                K2INFO("create config");
                return ConfigDist().start(config);  // initialize global config
            }()
                .then([&] {
                    K2INFO("create prometheus");
                    ConfigVar<uint16_t> promport{"prometheus_port"};

                    return prometheus.start(promport(), "K2 txbench client metrics", "txbench_client");
                })
                .then([&] {
                    K2INFO("create vnet");
                    return vnet.start();
                })
                .then([&]() {
                    K2INFO("create tcp");
                    return tcpproto.start(tcpProtobuilder);
                })
                .then([&]() {
                    K2INFO("create rdma");
                    return rrdmaproto.start(k2::RRDMARPCProtocol::builder(std::ref(vnet)));
                })
                .then([&]() {
                    K2INFO("create auto-rrdma proto");
                    return autoproto.start(k2::AutoRRDMARPCProtocol::builder(std::ref(vnet), std::ref(rrdmaproto)));
                })
                .then([&]() {
                    K2INFO("create dispatcher");
                    return RPCDist().start();
                })
                .then([&]() {
                    K2INFO("create user applets");
                    std::vector<seastar::future<>> ctorFutures;
                    for (auto& ctor : _ctors) {
                        ctorFutures.push_back(ctor());
                    }
                    return seastar::when_all(ctorFutures.begin(), ctorFutures.end()).discard_result();
                })
                // STARTUP LOGIC
                .then([&]() {
                    K2INFO("start VNS");
                    return vnet.invoke_on_all(&k2::VirtualNetworkStack::start);
                })
                .then([&]() {
                    K2INFO("start tcpproto");
                    return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&]() {
                    K2INFO("register TCP protocol");
                    // Could register more protocols here via separate invoke_on_all calls
                    return RPCDist().invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
                })
                .then([&]() {
                    K2INFO("start RDMA");
                    return rrdmaproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&]() {
                    K2INFO("register RDMA protocol");
                    // Could register more protocols here via separate invoke_on_all calls
                    return RPCDist().invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(rrdmaproto));
                })
                .then([&]() {
                    K2INFO("start auto-RRDMA protocol");
                    return autoproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&]() {
                    K2INFO("register auto-RRDMA protocol");
                    // Could register more protocols here via separate invoke_on_all calls
                    return RPCDist().invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(autoproto));
                })
                .then([&]() {
                    K2INFO("start dispatcher");
                    return RPCDist().invoke_on_all(&k2::RPCDispatcher::start);
                })
                .then([&]() {
                    K2INFO("start user applets");
                    std::vector<seastar::future<>> startFutures;
                    for (auto& starter : _starters) {
                        startFutures.push_back(starter());
                    }
                    return seastar::when_all(startFutures.begin(), startFutures.end()).discard_result();
                })
                .handle_exception([](auto exc) {
                    K2ERROR_EXC("Startup sequence failed with exception", exc);
                    throw exc;
                });
    });
    K2INFO("Shutdown was successful!");
    return result;
}
} // ns k2
