//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include <unordered_map>
#include <memory>
#include <functional>

// third-party
#include <seastar/core/distributed.hh> // for distributed<>
#include <seastar/core/app-template.hh> // for app_template
#include <seastar/core/future.hh> // for future stuff
#include <seastar/core/timer.hh> // periodic timer

// internal
#include "transport/RPCDispatcher.h"
#include "transport/TCPListener.h"
#include "transport/Log.h"
#include "transport/ListenerFactory.h"
#include "transport/VirtualFunction.h"

using namespace std::chrono_literals; // so that we can type "1ms"

// implements an example RPC Service which can send/receive messages
class Service {
public:
    // The message verbs supported by this service
    enum MsgVerbs: uint16_t {
        POST = 0,
        GET = 1
    };

    // distributed version of the class
    typedef seastar::distributed<Service> Dist_t;

    Service(k2tx::RPCDispatcher::Dist_t& dispatchers):
        _dispatchers(dispatchers),
        _updateTimer([this]{this->SendHeartbeat();}) {
        K2LOG("ctor");
    };

    virtual ~Service() {
        K2LOG("dtor");
        stop();
    }

    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2LOG("stop");
        _updateTimer.cancel();
        return seastar::make_ready_future<>();
    }

    // called after construction
    void Start() {
        K2LOG("Registering message handlers");
        // This method is executed on each object which distributed<> created for each core.
        // We want pull our thread-local dispatcher, and register ourselves to handle messages
        auto& disp = _dispatchers.local();
        disp.RegisterMessageHandler(MsgVerbs::POST, make_handler(handlePOST));
        disp.RegisterMessageHandler(MsgVerbs::GET, make_handler(handleGET));

        // also create a heartbeat timer
         _updateTimer.arm_periodic(1s);
    }

    void SendHeartbeat() {
        K2LOG("Sending heartbeat");
    }

public:
    // Message handlers
    void handlePOST(k2tx::Verb verb, k2tx::Payload& payload, k2tx::Channel& chan) {
        K2LOG("Received POST message: " << verb << ", from endpoint: " << chan.Endpoint() << ", with payload: " << getPayloadString(payload));
    }

    void handleGET(k2tx::Verb verb, k2tx::Payload& payload, k2tx::Channel& chan) {
        K2LOG("Received GET message: " << verb << ", from endpoint: " << chan.Endpoint() << ", with payload: " << getPayloadString(payload));
    }

private:
    static std::string getPayloadString(k2tx::Payload& payload) {
        std::string result;
        for (auto& fragment: payload.Fragments()) {
            result += std::string(fragment.get(), fragment.size());
        }
        return result;
    }
    k2tx::RPCDispatcher::Dist_t& _dispatchers;
    seastar::timer<> _updateTimer;

}; // class Service

int main(int ac, char** av) {
    // Services are constructed starting with the available VirtualFunctions(dpdk receive queues)
    // so that we have a stack on each core. The stack is:
    //  1 VF -> N Listeners -> 1 RPCDispatcher -> 1 ServiceHandler.
    // Note how we can have more than one listener per VF so that we can process say UDP and TCP packets at the
    // same time.
    // To build this, we use the seastar distributed<> mechanism, but we extend it so that we
    // limit the pool to the number of VFs we want to watch. The VFs are specified on the cmd line
    // and under the covers, seastar creates a pool of size=#VFs.
    k2tx::VirtualFunction::Dist_t vfs;
    k2tx::ListenerFactory::Dist_t listeners;
    k2tx::RPCDispatcher::Dist_t dispatchers;
    Service::Dist_t services;

    namespace bpo = boost::program_options;
    // create a seastar application. Note that this application handles a whole lot more command line options,
    // which are registered by seastar components (e.g. metrics, logging, reactor, network, etc)
    seastar::app_template app;
    app.add_options()
        ("tcp_port", bpo::value<uint16_t>()->default_value(2300), "TCP port to listen on");

    // we are now ready to assemble the running application
    return app.run_deprecated(ac, av, [&] {
        auto&& config = app.configuration();

        // call the stop() method on each object when we're about to exit. This also deletes the objects
        seastar::engine().at_exit([&] {
            K2LOG("");
            return services.stop();
        });
        seastar::engine().at_exit([&] {
            K2LOG("");
            return dispatchers.stop();
        });
        seastar::engine().at_exit([&] {
            K2LOG("");
            return listeners.stop();
        });
        seastar::engine().at_exit([&] {
            K2LOG("");
            return vfs.stop();
        });

        uint16_t tcp_port = config["tcp_port"].as<uint16_t>();

        return
            // These calls here actually call the constructor of each distributed class, once on
            // each core. This produces thread-local instances in each distributed<>
            // create and wire all objects. We do this by chaining a bunch of
            // futures which all complete pretty much instantly. The application
            // will remain running since the listeners should register
            // themselves to poll for new connections/packets.
            vfs.start()
                .then([&vfs, &listeners, tcp_port]() {
                    K2LOG("");
                    return listeners.start(k2tx::TCPListener::Builder(std::ref(vfs), tcp_port));
                })
                .then([&]() {
                    K2LOG("");
                    return dispatchers.start(std::ref(listeners));
                })
                .then([&]() {
                    K2LOG("");
                    return services.start(std::ref(dispatchers));
                })
                // once the objects have been constructed and wired, they can
                // all perform their startup logic
                .then([&]() {
                    K2LOG("");
                    return vfs.invoke_on_all(&k2tx::VirtualFunction::Start);
                })
                .then([&]() {
                    K2LOG("");
                    return listeners.invoke_on_all(&k2tx::ListenerFactory::Start);
                })
                .then([&]() {
                    K2LOG("");
                    return dispatchers.invoke_on_all(&k2tx::RPCDispatcher::Start);
                })
                .then([&]() {
                    K2LOG("");
                    return services.invoke_on_all(&Service::Start);
                });
    });
}
