//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <exception>
#include <chrono>
#include <string>
#include <cstdlib>
#include <ctime>

// third-party
#include <seastar/core/distributed.hh> // for distributed<>
#include <seastar/core/app-template.hh> // for app_template
#include <seastar/core/reactor.hh> // for app_template
#include <seastar/util/reference_wrapper.hh> // for app_template
#include <seastar/core/future.hh> // for future stuff
#include <seastar/core/timer.hh> // periodic timer
#include <seastar/core/metrics_registration.hh> // metrics
#include <seastar/core/metrics.hh>

using namespace std::chrono_literals; // so that we can type "1ms"
namespace bpo = boost::program_options;
namespace sm = seastar::metrics;

// k2 transport
#include <transport/RPCDispatcher.h>
#include <transport/TCPRPCProtocol.h>
#include <transport/RRDMARPCProtocol.h>
#include <common/Log.h>
#include <transport/BaseTypes.h>
#include <transport/RPCProtocolFactory.h>
#include <transport/VirtualNetworkStack.h>
#include <transport/RetryStrategy.h>
#include <transport/Prometheus.h>

#include <tso/service/TSOService.h>


int main(int argc, char** argv) {
    k2::VirtualNetworkStack::Dist_t vnet;
    k2::RPCProtocolFactory::Dist_t tcpproto;
    k2::RPCProtocolFactory::Dist_t rrdmaproto;
    k2::RPCDispatcher::Dist_t dispatcher;
    k2::TSOService::Dist_t tsoService;
    k2::Prometheus prometheus;

    seastar::app_template app;
    app.add_options()
        ("prometheus_port", bpo::value<uint16_t>()->default_value(8089), "HTTP port for the prometheus server");

    // we are now ready to assemble the running application
    auto result = app.run_deprecated(argc, argv, [&] {
        auto& config = app.configuration();
        uint16_t promport = config["prometheus_port"].as<uint16_t>();

        // call the stop() method on each object when we're about to exit. This also deletes the objects
        seastar::engine().at_exit([&] {
            K2INFO("prometheus stop");
            return prometheus.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("vnet stop");
            return vnet.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("tcpproto stop");
            return tcpproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("rrdma stop");
            return rrdmaproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("dispatcher stop");
            return dispatcher.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("TSOService stop");
            return tsoService.stop();
        });

        return
            // OBJECT CREATION (via distributed<>.start())
            [&]{
                K2INFO("start prometheus");
                return prometheus.start(promport, "TSOService metrics", "tso_service");
            }()
            .then([&] {
                K2INFO("create vnet");
                return vnet.start();
            })
            .then([&]() {
                K2INFO("create tcpproto");
                return tcpproto.start(k2::TCPRPCProtocol::builder(std::ref(vnet)));
            })
            .then([&]() {
                K2INFO("create RDMA");
                return rrdmaproto.start(k2::RRDMARPCProtocol::builder(std::ref(vnet)));
            })
            .then([&]() {
                K2INFO("create dispatcher");
                return dispatcher.start();
            })
            .then([&]() {
                K2INFO("create TSOService");
                return tsoService.start(std::ref(dispatcher), app.configuration());
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
                K2INFO("register TCP Protocol");
                // Could register more protocols here via separate invoke_on_all calls
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
            })
            .then([&]() {
                K2INFO("Start RDMA");
                return rrdmaproto.invoke_on_all(&k2::RPCProtocolFactory::start);
            })
            .then([&]() {
                K2INFO("register RDMA Protocol");
                // Could register more protocols here via separate invoke_on_all calls
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(rrdmaproto));
            })
            .then([&]() {
                K2INFO("start dispatcher");
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
            })
            .then([&]() {
                K2INFO("start TSOService");
                return tsoService.invoke_on_all(&k2::TSOService::start);
            });
    });
    K2INFO("Shutdown was successful!");
    return result;
}
