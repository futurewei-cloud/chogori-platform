//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <exception>
#include <chrono>
#include <cctype> // for is_print
#include <string>
using namespace std::chrono_literals; // so that we can type "1ms"

// third-party
#include <seastar/core/distributed.hh> // for distributed<>
#include <seastar/core/weak_ptr.hh> // for weak_ptr<>
#include <seastar/core/app-template.hh> // for app_template
#include <seastar/util/reference_wrapper.hh> // for app_template
#include <seastar/core/future.hh> // for future stuff
#include <seastar/core/timer.hh> // periodic timer

// k2 transport
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "common/Log.h"
#include "transport/BaseTypes.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "transport/RetryStrategy.h"


// implements an example RPC Service which can send/receive messages
class Service : public seastar::weakly_referencable<Service> {
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
    Service(k2::RPCDispatcher::Dist_t& dispatcher):
        _dispatcher(dispatcher),
        _updateTimer([this]{
            this->sendHeartbeat();
        }),
        _msgCount(0),
        _stopped(true) {
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
        _stopped = true;
        // unregistar all observers
        _dispatcher.local().registerMessageObserver(MsgVerbs::POST, nullptr);
        _dispatcher.local().registerMessageObserver(MsgVerbs::GET, nullptr);
        _dispatcher.local().registerMessageObserver(MsgVerbs::ACK, nullptr);
        _dispatcher.local().registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

    // called after construction
    void start() {
        assert(_stopped);
        // This method is executed on each object which distributed<> created for each core.
        // We want pull our thread-local dispatcher, and register ourselves to handle messages
        auto& disp = _dispatcher.local();

        // You can store the endpoint for more efficient communication
        _heartbeatTXEndpoint = disp.getTXEndpoint("tcp+k2rpc://127.0.0.1:14000");
        if (!_heartbeatTXEndpoint) {
            throw std::runtime_error("unable to get an endpoint for url");
        }

        K2INFO("Registering message handlers");

        disp.registerMessageObserver(MsgVerbs::POST,
            [this](k2::Request& request) mutable {
                this->handlePOST(request);
            });

        disp.registerMessageObserver(MsgVerbs::GET,
            [this](k2::Request& request) mutable {
                this->handleGET(request);
            });

        disp.registerMessageObserver(MsgVerbs::ACK,
            [this](k2::Request& request) mutable {
                auto received = getPayloadString(request.payload.get());
                K2INFO("Received ACK from " << request.endpoint.getURL() <<
                      ", and payload: " << received);
            });

        disp.registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            // we should release any payloads we're holding
            // ideally, we should release enough payloads whose size() sums up to more than requiredReleaseBytes
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        // also start the heartbeat timer
        _updateTimer.arm(_updateTimerInterval);
        _stopped = false;
    }

public: // Work generators
    void sendHeartbeat() {
        K2INFO("Sending reqid="<< _msgCount);
        // send a GET
        {
            k2::String msg("Requesting GET reqid=");
            msg += std::to_string(_msgCount++);

            std::unique_ptr<k2::Payload> request = _heartbeatTXEndpoint->newPayload();
            request->getWriter().write(msg.c_str(), msg.size()+1);
            // straight Send sends requests without any form of retry. Underlying transport may or may not
            // attempt redelivery (e.g. TCP packet reliability)
            _dispatcher.local().send(GET, std::move(request), *_heartbeatTXEndpoint);
        }

        // send a POST where we expect to receive a reply
        {
            _msgCount++;
            auto msground = _msgCount;

            // retry at 10ms, 50ms(=10ms*5), and 250ms(=50ms*5)
            auto retryStrategy = seastar::make_lw_shared<k2::ExponentialBackoffStrategy>();
            retryStrategy->withRetries(3).withStartTimeout(10ms).withRate(5);

            // NB: since seastar future continuations may be scheduled to run at later points,
            // it may be possible that the Service instance goes away in a middle of a retry.
            // To avoid a segmentation fault, either use copies, or as in this example - weak reference
            retryStrategy->run([self=weak_from_this(), msground](size_t retriesLeft, k2::Duration timeout) {
                K2INFO("Sending with retriesLeft=" << retriesLeft << ", and timeout="
                       << timeout.count() << ", in reqid="<< msground);
                if (!self) {
                    K2INFO("Stopping retry since dispatcher has exited");
                    return seastar::make_ready_future<>();
                }
                k2::String msgData = "Requesting POST reqid=";
                msgData += std::to_string(msground);
                // In this example, we must create a new payload each time we want to retry.
                // The reason is that once we attempt a send over a transport, we move ownership of payload
                // to the transport and may not be able to get the original packets back.
                // e.g. one we place the packets into the DPDK mem queue, we might not be able to obtain the
                // exact same packets back unless we do some cooperative refcounting with the dpdk internals
                std::unique_ptr<k2::Payload> msg = self->_heartbeatTXEndpoint->newPayload();
                msg->getWriter().write(msgData.c_str(), msgData.size()+1);

                // send a request with expected reply. Since we expect a reply, we must specify a timeout
                return self->_dispatcher.local().sendRequest(POST, std::move(msg), *self->_heartbeatTXEndpoint, timeout)
                .then([msground](std::unique_ptr<k2::Payload> payload) {
                    // happy case is chained right onto the dispatcher Send call
                    auto received = getPayloadString(payload.get());
                    K2INFO("Received reply for reqid=" << msground << " : " << received);
                    // if the application wants to retry again (in case of some retryable server error)
                    // ServiceMessage msg(payload);
                    // if (msg.Status != msg.StatusOK) {
                    //    return make_exception_future<>(std::exception(msg.Status));
                    // }
                });

            }) // Do returns an empty future here for the response either successful if any of the tries succeeded, or an exception.
            .handle_exception([msground](auto exc){
                // here we handle the exception case (e.g. timeout, unable to connect, invalid endpoint, etc)
                // this is the handler which handles the exception AFTER the retry strategy is exhausted
                K2ERROR("Failed to get response for message reqid=" << msground << " : " << exc);
            }).finally([self=weak_from_this(), retryStrategy, msground](){
                // to keep the retry strategy around while we're working on it, use a shared ptr and capture it
                // here by copy so that it is only released after Do completes.
                K2DEBUG("done with retry strategy for reqid=" << msground);
                if (self) {
                    // reschedule again since we're still alive
                    self->_updateTimer.arm(_updateTimerInterval);
                }
            });
        }
    }

public:
    // Message handlers
    void handlePOST(k2::Request& request) {
        auto received = getPayloadString(request.payload.get());
        K2INFO("Received POST message from endpoint: " << request.endpoint.getURL()
              << ", with payload: " << received);
        k2::String msgData("POST Message received reqid=");
        msgData += std::to_string(_msgCount++);

        std::unique_ptr<k2::Payload> msg = request.endpoint.newPayload();
        msg->getWriter().write(msgData.c_str(), msgData.size()+1);
        // respond to the client's request
        _dispatcher.local().sendReply(std::move(msg), request);
    }

    void handleGET(k2::Request& request) {
        auto received = getPayloadString(request.payload.get());
        K2INFO("Received GET message from endpoint: " << request.endpoint.getURL()
              << ", with payload: " << received);
        k2::String msgData("GET Message received reqid=");
        msgData += std::to_string(_msgCount++);

        std::unique_ptr<k2::Payload> msg = request.endpoint.newPayload();
        msg->getWriter().write(msgData.c_str(), msgData.size()+1);

        // Here we just forward the message using a straight Send and we don't expect any responses to our forward
        _dispatcher.local().send(ACK, std::move(msg), request.endpoint);
    }

private:
    static std::string getPayloadString(k2::Payload* payload) {
        if (!payload) {
            return "NO_PAYLOAD_RECEIVED";
        }
        std::string result;
        for (auto& binary: payload->release()) {
            K2DEBUG("Processing received binary of size=" << binary.size());
            auto datap = binary.get();
            for (size_t i = 0; i < binary.size(); ++i) {
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

    k2::RPCDispatcher::Dist_t& _dispatcher;
    seastar::timer<> _updateTimer;
    static constexpr auto _updateTimerInterval = 1s;
    std::unique_ptr<k2::TXEndpoint> _heartbeatTXEndpoint;
    // send around our message count
    uint64_t _msgCount;

    // flag we need to tell if we've been stopped
    bool _stopped;

}; // class Service

int main(int argc, char** argv) {
    // service are constructed starting with the available VirtualNetworkStacks(dpdk receive queues)
    // so that we have a stack on each core. The stack is:
    //  1 VF -> N protocols -> 1 RPCDispatcher -> 1 ServiceHandler.
    // Note how we can have more than one listener per VF so that we can process say UDP and TCP packets at the
    // same time.
    // To build this, we use the seastar distributed<> mechanism, but we extend it so that we
    // limit the pool to the number of network stacks we want to watch. The stacks are configured based on the cmd line options
    k2::VirtualNetworkStack::Dist_t vnet;
    k2::RPCProtocolFactory::Dist_t tcpproto;
    k2::RPCDispatcher::Dist_t dispatcher;
    Service::Dist_t service;

    namespace bpo = boost::program_options;
    // create a seastar application. Note that this application handles a whole lot more command line options,
    // which are registered by seastar components (e.g. metrics, logging, reactor, network, etc)
    seastar::app_template app;
    app.add_options()
        ("tcp_port", bpo::value<uint32_t>()->default_value(14000), "TCP port to listen on");

    // we are now ready to assemble the running application
    auto result = app.run_deprecated(argc, argv, [&] {
        auto&& config = app.configuration();

        // call the stop() method on each object when we're about to exit. This also deletes the objects
        seastar::engine().at_exit([&] {
            K2INFO("vnet stop");
            return vnet.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("tcpproto stop");
            return tcpproto.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("dispatcher stop");
            return dispatcher.stop();
        });
        seastar::engine().at_exit([&] {
            K2INFO("service stop");
            return service.stop();
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
                    return tcpproto.start(k2::TCPRPCProtocol::builder(std::ref(vnet), tcp_port));
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
                    return vnet.invoke_on_all(&k2::VirtualNetworkStack::start);
                })
                .then([&]() {
                    K2INFO("Start tcpproto");
                    return tcpproto.invoke_on_all(&k2::RPCProtocolFactory::start);
                })
                .then([&]() {
                    K2INFO("RegisterProtocol dispatcher");
                    // Could register more protocols here via separate invoke_on_all calls
                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::registerProtocol, seastar::ref(tcpproto));
                })
                .then([&]() {
                    K2INFO("Start dispatcher");
                    return dispatcher.invoke_on_all(&k2::RPCDispatcher::start);
                })
                .then([&]() {
                    K2INFO("Start service");
                    return service.invoke_on_all(&Service::start);
                });
    });
    K2INFO("Shutdown was successful!");
    return result;
}
