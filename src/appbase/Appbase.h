#pragma once

// stl
#include <string>

// third-party
#include <boost/program_options.hpp>
#include <seastar/core/app-template.hh>  // for app_template
#include <seastar/core/distributed.hh>   // for distributed<>
#include <seastar/core/future.hh>        // for future stuff
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>  // metrics
#include <seastar/core/reactor.hh>               // for access to reactor
#include <seastar/core/timer.hh>                 // periodic timer
#include <seastar/util/reference_wrapper.hh>     // for seastar::reference

// k2 transport
#include "common/Log.h"
#include "transport/Prometheus.h"
#include "transport/RPCDispatcher.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/RRDMARPCProtocol.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/VirtualNetworkStack.h"

namespace k2 {

using namespace std::chrono_literals;  // so that we can type "1ms"
namespace bpo = boost::program_options;
namespace sm = seastar::metrics;

// Helper class used to provide listening addresses for the TCP protocol
class MultiAddressProvider : public k2::IAddressProvider {
   public:
    MultiAddressProvider() = default;
    MultiAddressProvider(const std::vector<std::string>& urls) : _urls(urls) {}
    MultiAddressProvider& operator=(MultiAddressProvider&&) = default;
    seastar::socket_address getAddress(int coreID) const override {
        if (size_t(coreID) < _urls.size()) {
            K2DEBUG("Have url: " << coreID << ":" << _urls[coreID]);
            auto ep = k2::TXEndpoint::fromURL(_urls[coreID], nullptr);
            if (ep) {
                return seastar::socket_address(seastar::ipv4_addr(ep->getIP(), uint16_t(ep->getPort())));
            }
            // might not be in URL form (e.g. just a plain port)
            K2DEBUG("attempting to use url as a simple port");
            try {
                auto port = std::stoi(_urls[coreID]);
                return seastar::socket_address((uint16_t)port);
            }
            catch(...){}
            K2ASSERT(false, "Unable to construct Endpoint from URL: " << _urls[coreID]);
        }
        K2INFO("This core does not have a port assignment: " << coreID);
        return seastar::socket_address(seastar::ipv4_addr{0});
    }

   private:
    std::vector<std::string> _urls;
};  // class MultiAddressProvider

// This is a foundational class used to create K2 Apps.
template<typename UserAppType>
class App {
public:
    // Use this method to obtain a callable which is used to add additional command-line options (see how we use it below)
    bpo::options_description_easy_init addOptions() { return _app.add_options(); }

    // This method returns the distributed container for the UserAppType. This is useful if you want to
    // perform map/reduce type operations (see ss::distributed API)
    seastar::distributed<UserAppType>& getDist() { return _userApp;}

    // This method should be called to initialize the system.
    // During initialization, the constructor for UserAppType is called with the given ctorArgs.
    // Once all components are started, we call UserAppType::start() to let the user begin their workflow
    template<typename... Args>
    int start(int argc, char** argv, Args&&... ctorArgs) {
        k2::VirtualNetworkStack::Dist_t vnet;
        k2::RPCProtocolFactory::Dist_t tcpproto;
        k2::RPCProtocolFactory::Dist_t rrdmaproto;
        k2::Prometheus prometheus;
        MultiAddressProvider addrProvider;
        RPCProtocolFactory::BuilderFunc_t tcpProtobuilder;

        addOptions()
            ("prometheus_port", bpo::value<uint16_t>()->default_value(8089), "HTTP port for the prometheus server")
            ("tcp_port", bpo::value<uint16_t>(), "If specified, this TCP port will be opened on all shards (kernel-based incoming connection load-balancing via shared bind on same port from multiple listeners. Conflicts with --tcp_endpoints")
            ("tcp_endpoints", bpo::value<std::vector<std::string>>()->multitoken(), "A list(space-delimited) of TCP listening endpoints to assign to each core. You can specify either full endpoints, e.g. 'tcp+k2rpc://192.168.1.2:12345' or just ports , e.g. '12345'. If simple ports are specified, the stack will bind to 0.0.0.0. Conflicts with --tcp_port")
            ;

        // we are now ready to assemble the running application
        auto result = _app.run_deprecated(argc, argv, [&] {
            auto& config = _app.configuration();
            uint16_t promport = config["prometheus_port"].as<uint16_t>();

            if (config.count("tcp_port") && config.count("tcp_endpoints")) {
                const char* msg = "Only one of tcp_port/tcp_endpoints option is allowed";
                K2ERROR(msg)
                return seastar::make_exception_future<>(std::runtime_error(msg));
            }

            if (config.count("tcp_port")) {
                auto port = config["tcp_port"].as<uint16_t>();
                tcpProtobuilder = k2::TCPRPCProtocol::builder(std::ref(vnet), port);
            }
            else if (config.count("tcp_endpoints")) {
                std::vector<std::string> tcp_endpoints = config["tcp_endpoints"].as<std::vector<std::string>>();
                addrProvider = MultiAddressProvider(tcp_endpoints);
                tcpProtobuilder = k2::TCPRPCProtocol::builder(std::ref(vnet), std::ref(addrProvider));
            } else {
                tcpProtobuilder = k2::TCPRPCProtocol::builder(std::ref(vnet));
            }

            // call the stop() method on each object when we're about to exit. This also deletes the objects
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
                K2INFO("stop dispatcher");
                return RPC.stop();
            });
            seastar::engine().at_exit([&] {
                K2INFO("stop user app");
                return _userApp.stop();
            });

            return
                // OBJECT CREATION (via distributed<>.start())
                [&] {
                    K2INFO("create prometheus");
                    return prometheus.start(promport, "K2 txbench client metrics", "txbench_client");
                }()
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
                    K2INFO("create dispatcher");
                    return RPC.start();
                })
                .then([&]() {
                    K2INFO("create user app");
                    return _userApp.start(std::forward<Args>(ctorArgs)...);
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
                    return RPC.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
                })
                .then([&]() {
                    K2INFO("start RDMA");
                    return rrdmaproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&]() {
                    K2INFO("register RDMA protocol");
                    // Could register more protocols here via separate invoke_on_all calls
                    return RPC.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(rrdmaproto));
                })
                .then([&]() {
                    K2INFO("start dispatcher");
                    return RPC.invoke_on_all(&k2::RPCDispatcher::start);
                })
                .then([&]() {
                    K2INFO("start user app");
                    return _userApp.invoke_on_all(&UserAppType::start);
                });
        });
        K2INFO("Shutdown was successful!");
        return result;
    }

private:
    seastar::app_template _app;
    seastar::distributed<UserAppType> _userApp;
}; // class App

} // namespace k2
