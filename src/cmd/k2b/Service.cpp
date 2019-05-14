// std
# include <iostream>
#include <chrono>
// boost
#include "boost/program_options.hpp"
#include "boost/filesystem.hpp"
// seastar
#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
// k2
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"
// benchmarker
#include <benchmarker/Benchmarker.h>

namespace k2
{
namespace benchmarker
{
using namespace std::chrono_literals;

class Service : public seastar::weakly_referencable<Service>
{
private:
    k2::RPCDispatcher::Dist_t& _dispatcher;
public:
    typedef seastar::distributed<Service> Dist_t;
    std::unique_ptr<k2::TXEndpoint> _myEndpoint;
    const uint32_t _tcpPort;

    // The message verbs supported by this service
    enum MsgVerbs: uint8_t
    {
        POST = 100,
        GET = 101,
        ACK = 102
    };

    Service(k2::RPCDispatcher::Dist_t& dispatcher, uint32_t tcpPort)
    : _dispatcher(dispatcher)
    , _tcpPort(tcpPort)
    {
        // empty
    }

    ~Service()
    {
        // empty
    }

    void start()
    {
        _dispatcher.local().registerMessageObserver(MsgVerbs::GET,
            [this](k2::Request& request) mutable {
                this->handleGET(request);
            });

        // You can store the endpoint for more efficient communication
        _myEndpoint = _dispatcher.local().getTXEndpoint("tcp+k2rpc://127.0.0.1:" + std::to_string(_tcpPort));
        if (!_myEndpoint) {
            throw std::runtime_error("unable to get an endpoint for url");
        }
    }

    seastar::future<> stop()
    {
        return seastar::make_ready_future<>();
    }

    void handleGET(k2::Request& request)
    {
        auto received = getPayloadString(request.payload.get());
        K2INFO("Received GET message from endpoint: " << request.endpoint.getURL()
              << ", with payload: " << received);
        k2::String msgData("GET Message received reqid=");
        msgData += std::to_string(2);

        std::unique_ptr<k2::Payload> msg = request.endpoint.newPayload();
        msg->getWriter().write(msgData.c_str(), msgData.size()+1);

        // Here we just forward the message using a straight Send and we don't expect any responses to our forward
        _dispatcher.local().sendReply(std::move(msg), request);
    }

private:
  static std::string getPayloadString(k2::Payload* payload) {
        if (!payload) {
            return "NO_PAYLOAD_RECEIVED";
        }
        std::string result;
        for (auto& fragment: payload->release()) {
            K2DEBUG("Processing received fragment of size=" << fragment.size());
            auto datap = fragment.get();
            for (size_t i = 0; i < fragment.size(); ++i) {
                if (std::isprint(static_cast<unsigned char>(datap[i]))) {
                    result.append(1, datap[i]);
                }
                else {
                    result.append(1, '.');
                }
            }
        }
        return result;
    }

}; // Service

}; // benchmarker

}; // k2

//
// K2 Benchmarker entry point.
//
int main(int argc, char** argv)
{
    namespace bpo = boost::program_options;
    uint32_t tcpPort = 14000;

    k2::VirtualNetworkStack::Dist_t virtualNetwork;
    k2::RPCProtocolFactory::Dist_t protocolFactory;
    k2::RPCDispatcher::Dist_t dispatcher;
    k2::benchmarker::Service::Dist_t service;

    seastar::app_template app;
    app.add_options()
        ("tcp_port", bpo::value<uint32_t>()->default_value(tcpPort), "TCP port to listen on");

    return app.run_deprecated(argc, (char**)argv, [&] {
        seastar::engine().at_exit([&] {
            return service.stop()
                .then([&] {
                    return dispatcher.stop();
                })
                .then([&] {
                    return protocolFactory.stop();
                })
                .then([&] {
                    return virtualNetwork.stop();
                });
        });

        auto&& config = app.configuration();
        tcpPort = config["tcp_port"].as<uint32_t>();

        return virtualNetwork.start()
            .then([&] {

                return protocolFactory.start(k2::TCPRPCProtocol::builder(std::ref(virtualNetwork), tcpPort));
            })
            .then([&] {
                return dispatcher.start();
            })
            .then([&] {
                return service.start(std::ref(dispatcher), tcpPort);
            })
            .then([&] {
                return virtualNetwork.invoke_on_all(&k2::VirtualNetworkStack::start);
            })
            .then([&] {
                return protocolFactory.invoke_on_all(&k2::RPCProtocolFactory::start);
            })
            .then([&] {
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(protocolFactory));
            })
            .then([&] {
                return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
            })
            .then([&] {
                return service.invoke_on_all(&k2::benchmarker::Service::start);
            });
    });
}
