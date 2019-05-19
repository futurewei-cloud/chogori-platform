// std
# include <iostream>
#include <chrono>
// boost
#include "boost/program_options.hpp"
#include "boost/filesystem.hpp"
// seastar
#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
// k2
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"
// k2b
#include <benchmarker/Benchmarker.h>

using namespace std::chrono_literals;

namespace k2
{
namespace benchmarker
{

class Client : public seastar::weakly_referencable<Client>
{
private:
    k2::RPCDispatcher::Dist_t& _dispatcher;
    std::unique_ptr<k2::TXEndpoint> _serviceEndpoint;
    const uint32_t _tcpPort;
public:
    typedef seastar::distributed<Client> Dist_t;

    // The message verbs supported by this service
    enum MsgVerbs: Verb
    {
        POST = 100,
        GET = 101,
        ACK = 102
    };

    Client(k2::RPCDispatcher::Dist_t& dispatcher, uint32_t tcpPort)
    : _dispatcher(dispatcher)
    , _tcpPort(tcpPort)
    {
        // empty
    }

    ~Client()
    {
        // empty
    }

    void start()
    {
        _serviceEndpoint = _dispatcher.local().getTXEndpoint("tcp+k2rpc://127.0.0.1:" + std::to_string(_tcpPort));
        if (!_serviceEndpoint) {
            throw std::runtime_error("unable to get an endpoint for url");
        }

        getHeartbeat();
    }

    seastar::future<> stop()
    {
        return seastar::make_ready_future<>();
    }

    void getHeartbeat() {
        k2::String msg("Requesting GET reqid=");
        msg += std::to_string(1);

        std::unique_ptr<k2::Payload> request = _serviceEndpoint->newPayload();
        request->getWriter().write(msg.c_str(), msg.size()+1);

        _dispatcher.local().sendRequest(GET, std::move(request), *_serviceEndpoint, 20s)
        .then([&](std::unique_ptr<k2::Payload> payload) {
            auto received = getPayloadString(payload.get());
            K2INFO("Received GET message from endpoint: " << _serviceEndpoint->getURL() << ", with payload: " << received);
        })
        .handle_exception([this](auto ex){
            K2INFO("Exception: ex" << ex);
        })
        .finally([this] {
            getHeartbeat();
        });
    }

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

}; // Client

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
    k2::benchmarker::Client::Dist_t client;

    seastar::app_template app;
    app.add_options()
        ("tcp_port", bpo::value<uint32_t>()->default_value(tcpPort), "TCP port to listen on");

    return app.run_deprecated(argc, (char**)argv, [&] {
        seastar::engine().at_exit([&] {
            return client.stop()
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
                return protocolFactory.start(k2::TCPRPCProtocol::builder(std::ref(virtualNetwork), 2021));
            })
            .then([&] {
                return dispatcher.start();
            })
            .then([&] {
                return client.start(std::ref(dispatcher), tcpPort);
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
                return client.invoke_on_all(&k2::benchmarker::Client::start);
            });
    });
}
