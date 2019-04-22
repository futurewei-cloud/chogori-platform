//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <exception>
#include <chrono>
using namespace std::chrono_literals; // so that we can type "1ms"

// third-party
#include <seastar/core/distributed.hh> // for distributed<>
#include <seastar/core/app-template.hh> // for app_template
#include <seastar/util/reference_wrapper.hh> // for app_template
#include <seastar/core/future.hh> // for future stuff
#include <seastar/core/timer.hh> // periodic timer

// k2 transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/Log.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"


// implements an example RPC Service which can send/receive messages
class Service {
public: // public types
    // The message verbs supported by this service
    enum MsgVerbs: uint8_t {
        POST = 100,
        GET = 101,
        ACK = 102
    };

    // distributed version of the class
    typedef seastar::distributed<Service> Dist_t;

public:  // application lifespan
    Service(k2tx::RPCDispatcher::Dist_t& dispatcher):
        _dispatcher(dispatcher),
        _updateTimer([this]{
            this->SendHeartbeat();
        }) {
        // Constructors should not do much more than just remembering the passed state because
        // not all dependencies may have been created yet.
        // The initialization should happen in Start() since at that point all deps should have been created
        K2INFO("ctor");
    };

    virtual ~Service() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2INFO("stop");
        _updateTimer.cancel();
        return seastar::make_ready_future<>();
    }

    // called after construction
    void Start() {
        // This method is executed on each object which distributed<> created for each core.
        // We want pull our thread-local dispatcher, and register ourselves to handle messages
        auto& disp = _dispatcher.local();

        // You can store the endpoint for more efficient communication
        _heartbeatEndpoint = disp.GetEndpoint("tcp+k2rpc://127.0.0.1:14000");
        if (!_heartbeatEndpoint) {
            throw std::runtime_error("unable to get an endpoint for url");
        }

        K2INFO("Registering message handlers");

        disp.RegisterMessageObserver(MsgVerbs::POST,
            [this](k2tx::Request request) mutable {
                this->handlePOST(std::move(request));
            });

        disp.RegisterMessageObserver(MsgVerbs::GET,
            [this](k2tx::Request request) mutable {
                this->handleGET(std::move(request));
            });

        disp.RegisterMessageObserver(MsgVerbs::ACK,
            [this](k2tx::Request request) mutable {
                K2INFO("Received ACK from " << request.endpoint.GetURL() <<
                      ", and payload: " << GetPayloadString(request.payload.get()));
            });

        disp.RegisterLowTransportMemoryObserver([](const k2tx::String& ttype, size_t requiredReleaseBytes) {
            // we should release any payloads we're holding
            // ideally, we should release enough payloads whose size() sums up to more than requiredReleaseBytes
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        // also start the heartbeat timer
        _updateTimer.arm_periodic(1s);
    }

public: // Work generators
    void SendHeartbeat() {
        K2INFO("Sending heartbeat");
        auto& disp = _dispatcher.local();
        // send a GET
        {
            k2tx::String msg("Requesting GET");
            std::unique_ptr<k2tx::Payload> request = _heartbeatEndpoint->NewPayload();
            request->Append(msg.c_str(), msg.size()+1);
            // straight Send sends requests without any form of retry. Underlying transport may or may not
            // attempt redelivery (e.g. TCP packet reliability)
            disp.Send(GET, std::move(request), *_heartbeatEndpoint.get());
        }

        // send a POST where we expect to receive a reply
        {
            // retry at 1ms, 5ms(=1ms*5), and 25ms(=5ms*5)
            k2tx::ExponentialBackoffStrategy retryStrategy;
            retryStrategy.WithRetries(3).WithStartTimeout(1ms).WithRate(5);
            retryStrategy.Do([this, &disp](size_t retriesLeft, k2tx::Duration timeout) {
                K2INFO("Sending with retriesLeft=" << retriesLeft << ", and timeout=" << timeout.count());
                k2tx::String msgData = "Requesting POST";
                // In this example, we must create a new payload each time we want to retry.
                // The reason is that once we attempt a send over a transport, we move ownership of payload
                // to the transport and may not be able to get the original packets back.
                std::unique_ptr<k2tx::Payload> msg = _heartbeatEndpoint->NewPayload();
                msg->Append(msgData.c_str(), msgData.size()+1);

                // send a request with expected reply. Since we expect a reply, we must specify a timeout
                return disp.SendRequest(POST, std::move(msg), *_heartbeatEndpoint.get(), timeout)
                .then([this](std::unique_ptr<k2tx::Payload> payload) {
                    K2INFO("Received reply for message: " << GetPayloadString(payload.get()));
                    // if the application wants to retry again (in case of some retryable server error)
                    // ServiceMessage msg(payload);
                    // if (msg.Status != msg.StatusOK) {
                    //    return make_exception_future<>(std::exception(msg.Status));
                    // }
                });
            }).handle_exception([](auto exc){
                K2ERROR("Failed to get response for message: " << exc);
            });
        }
    }

public:
    // Message handlers
    void handlePOST(k2tx::Request request) {
        K2INFO("Received POST message from endpoint: " << request.endpoint.GetURL()
              << ", with payload: " << GetPayloadString(request.payload.get()) );
        k2tx::String msgData("POST Message received");
        std::unique_ptr<k2tx::Payload> msg = request.endpoint.NewPayload();
        msg->Append(msgData.c_str(), msgData.size()+1);

        // respond to the client's request
        _dispatcher.local().SendReply(std::move(msg), std::move(request));
    }

    void handleGET(k2tx::Request request) {
        K2INFO("Received GET message from endpoint: " << request.endpoint.GetURL()
              << ", with payload: " << GetPayloadString(request.payload.get()) );
        k2tx::String msgData("GET Message received");
        std::unique_ptr<k2tx::Payload> msg = request.endpoint.NewPayload();
        msg->Append(msgData.c_str(), msgData.size()+1);

        // Here we just forward the message using a straight Send and we don't expect any responses to our forward
        _dispatcher.local().Send(ACK, std::move(msg), request.endpoint);
    }

private:
    static k2tx::String GetPayloadString(k2tx::Payload* payload) {
        if (!payload) {
            return "NO_PAYLOAD_RECEIVED";
        }
        k2tx::String result;
        for (auto& fragment: payload->Fragments()) {
            result += k2tx::String(fragment.get(), fragment.size());
        }
        return result;
    }

    k2tx::RPCDispatcher::Dist_t& _dispatcher;
    seastar::timer<> _updateTimer;
    std::unique_ptr<k2tx::Endpoint> _heartbeatEndpoint;

}; // class Service

int main(int argc, char** argv) {
    // service are constructed starting with the available VirtualNetworkStacks(dpdk receive queues)
    // so that we have a stack on each core. The stack is:
    //  1 VF -> N protocols -> 1 RPCDispatcher -> 1 ServiceHandler.
    // Note how we can have more than one listener per VF so that we can process say UDP and TCP packets at the
    // same time.
    // To build this, we use the seastar distributed<> mechanism, but we extend it so that we
    // limit the pool to the number of network stacks we want to watch. The stacks are configured based on the cmd line options
    k2tx::VirtualNetworkStack::Dist_t vnet;
    k2tx::RPCProtocolFactory::Dist_t tcpproto;
    k2tx::RPCDispatcher::Dist_t dispatcher;
    Service::Dist_t service;

    namespace bpo = boost::program_options;
    // create a seastar application. Note that this application handles a whole lot more command line options,
    // which are registered by seastar components (e.g. metrics, logging, reactor, network, etc)
    seastar::app_template app;
    app.add_options()
        ("tcp_port", bpo::value<uint32_t>()->default_value(14000), "TCP port to listen on");

    // we are now ready to assemble the running application
    return app.run_deprecated(argc, argv, [&] {
        auto&& config = app.configuration();

        // call the stop() method on each object when we're about to exit. This also deletes the objects
        seastar::engine().at_exit([&] {
            K2INFO("service stop");
            return service.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("dispatcher stop");
            return dispatcher.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("tcpproto stop");
            return tcpproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("vnet stop");
            return vnet.stop();
        });

        uint32_t tcp_port = config["tcp_port"].as<uint32_t>();

        return
            // These calls here actually call the constructor of each distributed class, once on
            // each core. This produces thread-local instances in each distributed<>
            // create and wire all objects. We do this by chaining a bunch of
            // futures which all complete pretty much instantly.
            // The application remains running since various components register recurring events
            // There are also some internal components which register recurring events (e.g. metrics http server)
            vnet.start()
                .then([&vnet, &tcpproto, tcp_port]() {
                    K2INFO("start tcpproto");
                    return tcpproto.start(k2tx::TCPRPCProtocol::Builder(std::ref(vnet), tcp_port));
                })
                .then([&]() {
                    K2INFO("start dispatcher");
                    return dispatcher.start();
                })
                .then([&]() {
                    K2INFO("start service");
                    return service.start(std::ref(dispatcher));
                })
                // once the objects have been constructed and wired, they can
                // all perform their startup logic
                .then([&]() {
                    K2INFO("Start VNS");
                    return vnet.invoke_on_all(&k2tx::VirtualNetworkStack::Start);
                })
                .then([&]() {
                    K2INFO("Start tcpproto");
                    return tcpproto.invoke_on_all(&k2tx::RPCProtocolFactory::Start);
                })
                .then([&]() {
                    K2INFO("RegisterProtocol dispatcher");
                    // Could register more protocols here via separate invoke_on_all calls
                    return dispatcher.invoke_on_all(&k2tx::RPCDispatcher::RegisterProtocol, seastar::ref(tcpproto));
                })
                .then([&]() {
                    K2INFO("Start dispatcher");
                    return dispatcher.invoke_on_all(&k2tx::RPCDispatcher::Start);
                })
                .then([&]() {
                    K2INFO("Start service");
                    return service.invoke_on_all(&Service::Start);
                });
    });
}
