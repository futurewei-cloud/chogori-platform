#pragma once

// stl
#include <string>

// third-party
#include <boost/program_options.hpp>
#include <boost/pointer_cast.hpp>
#include <seastar/core/app-template.hh>  // for app_template

// k2 base
#include <k2/common/TypeMap.h>

// k2 transport
#include <k2/transport/RPCProtocolFactory.h>
#include <k2/transport/RRDMARPCProtocol.h>
#include <k2/transport/TCPRPCProtocol.h>
#include <k2/transport/VirtualNetworkStack.h>

#include "AppEssentials.h"

namespace k2 {

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
            } catch (...) {
            }
            K2ASSERT(false, "Unable to construct Endpoint from URL: " << _urls[coreID]);
        }
        K2INFO("This core does not have a port assignment: " << coreID);
        return seastar::socket_address(seastar::ipv4_addr{0});
    }

   private:
    std::vector<std::string> _urls;
};  // class MultiAddressProvider

// This is a foundational class used to create K2 Apps.
class App {
   public:  // API
    // helper class for positional option adding
    class PosOptAdder {
       public:
        PosOptAdder(App* app) : _app(app) {}
        PosOptAdder& operator()(const char* name, const bpo::value_semantic* value_semantic, const char* help, int max_count) {
            _app->_app.add_positional_options({{name, value_semantic, help, max_count}});
            return *this;
        }

       private:
        App* _app;
    };
    // Use this method to obtain a callable which can be used to add additional
    // command-line options (see how we use it below)
    bpo::options_description_easy_init addOptions() { return _app.add_options(); }

    // Use this method to add positional options. The method returns an option applier much like getOptions() above
    PosOptAdder addPositionalOptions() {
        return PosOptAdder(this);
    }

    // This method returns the distributed container for the ActivityType.
    // This container can then be used to perform map/reduce type operations (see ss::distributed API)
    template <typename ActivityType>
    seastar::distributed<ActivityType>& getDist() {
        auto findIter = _activities.find<ActivityType>();
        if (findIter == _activities.end()) {
            throw std::runtime_error("activity not found");
        }
        // this is safe, since we created the object ourselves, based on ActivityType
        // in other words, if we found an entry of type ActivityType, then the value stored in the map
        // is guaranteed to be of type seastar::distributed<ActivityType>*
        return *(static_cast<seastar::distributed<ActivityType>*>(findIter->second));
    }

    // Add a activity to th
    template <typename ActivityType, typename... ConstructorArgs>
    void addActivity(ConstructorArgs&&... ctorArgs) {
        if (_activities.find<ActivityType>() != _activities.end()) {
            throw std::runtime_error("duplicate activities not allowed");
        }

        seastar::distributed<ActivityType>* dd = new seastar::distributed<ActivityType>();
        _ctors.push_back(
            [dd, args = std::make_tuple(std::forward<ConstructorArgs>(ctorArgs)...)]() mutable {
                // Start the distributed container which will construct the objects on each core
                return std::apply(
                    [dd](ConstructorArgs&&... args) {
                        return dd->start(std::forward<ConstructorArgs>(args)...);
                    },
                    std::move(args));
            });
        _starters.push_back([dd]() mutable { return dd->invoke_on_all(&ActivityType::start); });
        _stoppers.push_back([dd]() mutable { return dd->stop(); });
        _dtors.push_back([dd]() mutable { delete dd; });

        // type-erase the container and put it in the map.
        _activities.put<ActivityType>((void*)dd);
    }

    // This method should be called to initialize the system.
    // 1. All activities are constructed with the arguments supplied when addActivity() was called
    // Once all components are started, we call ActivityTypes::start() to let the user begin their workflow
    int start(int argc, char** argv) {
        k2::VirtualNetworkStack::Dist_t vnet;
        k2::RPCProtocolFactory::Dist_t tcpproto;
        k2::RPCProtocolFactory::Dist_t rrdmaproto;
        k2::Prometheus prometheus;
        MultiAddressProvider addrProvider;
        RPCProtocolFactory::BuilderFunc_t tcpProtobuilder;

        addOptions()("prometheus_port", bpo::value<uint16_t>()->default_value(8089), "HTTP port for the prometheus server")("tcp_port", bpo::value<uint16_t>(), "If specified, this TCP port will be opened on all shards (kernel-based incoming connection load-balancing via shared bind on same port from multiple listeners. Conflicts with --tcp_endpoints")("tcp_endpoints", bpo::value<std::vector<std::string>>()->multitoken(), "A list(space-delimited) of TCP listening endpoints to assign to each core. You can specify either full endpoints, e.g. 'tcp+k2rpc://192.168.1.2:12345' or just ports , e.g. '12345'. If simple ports are specified, the stack will bind to 0.0.0.0. Conflicts with --tcp_port");

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

            uint16_t promport = config["prometheus_port"].as<uint16_t>();

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
                K2INFO("stop dispatcher");
                return RPCDist().stop();
            });
            seastar::engine().at_exit([&] {
                K2INFO("stop user activities");
                std::vector<seastar::future<>> stopFutures;
                for (auto& stopper : _stoppers) {
                    stopFutures.push_back(stopper());
                }
                return seastar::when_all(stopFutures.begin(), stopFutures.end()).then([](auto) { return seastar::make_ready_future(); });
            });

            return
                // OBJECT CREATION (via distributed<>.start())
                [&] {
                    K2INFO("create config");
                    return ConfigDist().start(config);  // initialize global config
                }()
                    .then([&] {
                        K2INFO("create prometheus");
                        return prometheus.start(promport, "K2 txbench client metrics", "txbench_client");
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
                        K2INFO("create dispatcher");
                        return RPCDist().start();
                    })
                    .then([&]() {
                        K2INFO("create user activities");
                        std::vector<seastar::future<>> ctorFutures;
                        for (auto& ctor : _ctors) {
                            ctorFutures.push_back(ctor());
                        }
                        return seastar::when_all(ctorFutures.begin(), ctorFutures.end()).then([](auto) { return seastar::make_ready_future(); });
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
                        K2INFO("start dispatcher");
                        return RPCDist().invoke_on_all(&k2::RPCDispatcher::start);
                    })
                    .then([&]() {
                        K2INFO("start user activities");
                        std::vector<seastar::future<>> startFutures;
                        for (auto& starter : _starters) {
                            startFutures.push_back(starter());
                        }
                        return seastar::when_all(startFutures.begin(), startFutures.end())
                            .then([](auto) { return seastar::make_ready_future(); });
                    });
        });
        K2INFO("Shutdown was successful!");
        return result;
    }

    ~App() {
        for (auto&& dtor : _dtors) {
            dtor();
        }
    }

   private:
    seastar::app_template _app;
    TypeMap<void*> _activities;
    std::vector<std::function<seastar::future<>()>> _ctors;     // functors which create user activities
    std::vector<std::function<seastar::future<>()>> _starters;  // functors which call start() on user activities
    std::vector<std::function<seastar::future<>()>> _stoppers;  // functors which call stop() on user activities
    std::vector<std::function<void()>> _dtors;                  // functors which delete user activities
};                                                              // class App

} // namespace k2
