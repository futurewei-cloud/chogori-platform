//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <cstdlib>
#include <ctime>
#include <unordered_map>

// third-party
#include <seastar/core/distributed.hh> // for distributed<>
#include <seastar/core/weak_ptr.hh> // for weak_ptr<>
#include <seastar/core/app-template.hh> // for app_template
#include <seastar/util/reference_wrapper.hh> // for app_template
#include <seastar/core/future.hh> // for future stuff

namespace bpo = boost::program_options;

// k2 transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/RRDMARPCProtocol.h"
#include "transport/TXEndpoint.h"
#include "common/Log.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/Prometheus.h"

#include "txbench_common.h"

class Service : public seastar::weakly_referencable<Service> {
public: // public types
    // distributed version of the class
    typedef seastar::distributed<Service> Dist_t;

public:  // application lifespan
    Service(k2::RPCDispatcher::Dist_t& dispatcher):
        _disp(dispatcher.local()),
        _stopped(true) {
        K2INFO("ctor");
    };

    virtual ~Service() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2INFO("stop");
        _stopped = true;
        // unregistar all observers
        _disp.registerMessageObserver(GET_DATA_URL, nullptr);
        _disp.registerMessageObserver(REQUEST, nullptr);
        _disp.registerMessageObserver(START_SESSION, nullptr);
        _disp.registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        _stopped = false;

        _registerDATA_URL();
        _registerSTART_SESSION();
        _registerREQUEST();

        _disp.registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        return seastar::make_ready_future<>();
    }

private:
    void _registerDATA_URL() {
        K2INFO("TCP endpoint is: " << _disp.getServerEndpoint(k2::TCPRPCProtocol::proto)->getURL());
        _disp.registerMessageObserver(GET_DATA_URL,
            [this](k2::Request&& request) mutable {
                auto response = request.endpoint.newPayload();
                auto ep = (seastar::engine()._rdma_stack?
                           _disp.getServerEndpoint(k2::RRDMARPCProtocol::proto):
                           _disp.getServerEndpoint(k2::TCPRPCProtocol::proto));
                K2INFO("GET_DATA_URL responding with data endpoint: " << ep->getURL());
                response->getWriter().write((void*)ep->getURL().c_str(), ep->getURL().size());
                _disp.sendReply(std::move(response), request);
            });
    }

    void _registerSTART_SESSION() {
        _disp.registerMessageObserver(START_SESSION, [this](k2::Request&& request) mutable {
            auto sid = uint64_t(std::rand());
            if (request.payload) {
                auto rdr = request.payload->getReader();
                SessionConfig config{};
                rdr.read((void*)&config, sizeof(config));

                BenchSession session(request.endpoint, sid, config);
                auto result = _sessions.try_emplace(sid, std::move(session));
                assert(result.second);
                K2INFO("Starting new session: " << sid);
                auto resp = request.endpoint.newPayload();
                SessionAck ack{.sessionID=sid};
                resp->getWriter().write((void*)&ack, sizeof(ack));
                _disp.sendReply(std::move(resp), request);
            }
        });
    }

    void _registerREQUEST() {
        _disp.registerMessageObserver(REQUEST, [this](k2::Request&& request) mutable {
            if (request.payload) {
                auto rdr = request.payload->getReader();
                uint64_t sid = 0;
                uint64_t reqId = 0;
                rdr.read((void*)&sid, sizeof(sid));
                rdr.read((void*)&reqId, sizeof(reqId));

                auto siditer = _sessions.find(sid);
                if (siditer != _sessions.end()) {
                    auto& session = siditer->second;
                    session.runningSum += reqId;
                    session.totalSize += session.config.responseSize;
                    session.totalCount += 1;
                    session.unackedSize += session.config.responseSize;
                    session.unackedCount += 1;
                    if (session.config.echoMode) {
                        auto&& buffs = request.payload->release();
                        _data.insert(_data.end(), std::move_iterator(buffs.begin()), std::move_iterator(buffs.end()));
                    }
                    if (session.unackedCount >= session.config.ackCount) {
                        auto response = request.endpoint.newPayload();
                        Ack ack{.sessionID=sid, .totalCount=session.totalCount, .totalSize=session.totalSize, .checksum=session.runningSum};
                        response->getWriter().write((void*)&ack, sizeof(ack));
                        if (session.config.echoMode) {
                            for (auto&& buf: _data) {
                                response->getWriter().write(buf.get(), buf.size());
                            }
                            _data.clear();
                        }
                        _disp.send(ACK, std::move(response), request.endpoint);
                        session.unackedSize = 0;
                        session.unackedCount = 0;
                    }
                }
            }
        });
    }

private:
    k2::RPCDispatcher& _disp;

    // flag we need to tell if we've been stopped
    bool _stopped;

    std::unordered_map<uint64_t, BenchSession> _sessions;

    std::vector<k2::Binary> _data;

}; // class Service

class TXBenchAddressProvider: public k2::IAddressProvider {
public:
    TXBenchAddressProvider() = default;
    TXBenchAddressProvider(const std::vector<std::string>& urls): _urls(urls){}
    TXBenchAddressProvider& operator=(TXBenchAddressProvider&&) = default;
    seastar::socket_address getAddress(int coreID) const override {
        if (size_t(coreID) < _urls.size()) {
            K2DEBUG("Have url: " << coreID << ":" << _urls[coreID]);
            auto ep = k2::TXEndpoint::fromURL(_urls[coreID], nullptr);
            if (!ep) {
                K2ASSERT(false, "Unable to construct Endpoint from URL: " << _urls[coreID]);
            }
            return seastar::socket_address(seastar::ipv4_addr(ep->getIP(), uint16_t(ep->getPort())));
        }
        K2DEBUG("This core does not have a port: " << coreID);
        return seastar::socket_address(seastar::ipv4_addr{0});
    }

private:
    std::vector<std::string> _urls;
}; // class TXBenchAddressProvider

int main(int argc, char** argv) {
    std::srand(std::time(nullptr));
    k2::VirtualNetworkStack::Dist_t vnet;
    k2::RPCProtocolFactory::Dist_t tcpproto;
    k2::RPCProtocolFactory::Dist_t rrdmaproto;
    k2::RPCDispatcher::Dist_t dispatcher;
    Service::Dist_t service;
    k2::Prometheus prometheus;
    TXBenchAddressProvider addrProvider;

    seastar::app_template app;
    app.add_options()
        ("prometheus_port", bpo::value<uint16_t>()->default_value(8088), "HTTP port for the prometheus server")
        ("tcp_urls", bpo::value<std::vector<std::string>>()->multitoken(), "TCP urls to listen on");

    // we are now ready to assemble the running application
    auto result = app.run_deprecated(argc, argv, [&] {
        auto& config = app.configuration();
        std::vector<std::string> tcp_urls = config["tcp_urls"].as<std::vector<std::string>>();
        uint16_t promport = config["prometheus_port"].as<uint16_t>();
        addrProvider = TXBenchAddressProvider(tcp_urls);

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
            K2INFO("service stop");
            return service.stop();
        });

        return
            // OBJECT CREATION (via distributed<>.start())
            [&]{
                K2INFO("Start prometheus");
                return prometheus.start(promport, "K2 txbench server metrics", "txbench_server");
            }()
            .then([&] {
                K2INFO("create vnet");
                return vnet.start();
            })
            .then([&]() {
                K2INFO("create tcpproto");
                return tcpproto.start(k2::TCPRPCProtocol::builder(std::ref(vnet), std::ref(addrProvider)));
            })
            .then([&]() {
                K2INFO("Service started... create RDMA");
                return rrdmaproto.start(k2::RRDMARPCProtocol::builder(std::ref(vnet)));
            })
            .then([&]() {
                K2INFO("create dispatcher");
                return dispatcher.start();
            })
            .then([&]() {
                K2INFO("create service");
                return service.start(std::ref(dispatcher));
            })
            // STARTUP LOGIC
            .then([&]() {
                K2INFO("Start VNS");
                return vnet.invoke_on_all(&k2::VirtualNetworkStack::start);
            })
            .then([&]() {
                K2INFO("Start tcpproto");
                return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
            })
            .then([&]() {
                K2INFO("Register TCP Protocol");
                // Could register more protocols here via separate invoke_on_all calls
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
            })
            .then([&]() {
                K2INFO("Start RDMA");
                return rrdmaproto.invoke_on_all(&k2::RPCProtocolFactory::start);
            })
            .then([&]() {
                K2INFO("Register RDMA Protocol");
                // Could register more protocols here via separate invoke_on_all calls
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(rrdmaproto));
            })
            .then([&]() {
                K2INFO("Start dispatcher");
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
            })
            .then([&]() {
                K2INFO("service start");
                return service.invoke_on_all(&Service::start);
            });
    });
    K2INFO("Shutdown was successful!");
    return result;
}
