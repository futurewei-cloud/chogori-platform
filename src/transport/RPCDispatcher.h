//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <functional>
#include <unordered_map>

#include <seastar/core/distributed.hh>

#include "ListenerFactory.h"
#include "Payload.h"
#include "Channel.h"

#define make_handler(h) {[&](k2tx::Verb verb, k2tx::Payload& payload, k2tx::Channel& channel) mutable { (h)(verb, payload, channel); }}

namespace k2tx {
// The type for verbs in the RPC system
typedef uint16_t Verb;

// The type of a message handler
// TODO Use something other than std::function due to dynamic allocation on
// create and runtime pointer dereference to run (no inline available) for 20ns penalty per call
// See https://www.boost.org/doc/libs/1_69_0/doc/html/function/faq.html
using MessageHandler_t = std::function<void(Verb, Payload&, Channel&)>;

// An RPC dispatcher is the interaction point between a service application and underlying transport
class RPCDispatcher {
public:
    RPCDispatcher(ListenerFactory::Dist_t& listeners);
    RPCDispatcher(const RPCDispatcher& o) = default;
    RPCDispatcher(RPCDispatcher&& o) = default;
    RPCDispatcher &operator=(const RPCDispatcher& o) = default;
    RPCDispatcher &operator=(RPCDispatcher&& o) = default;
    virtual ~RPCDispatcher();
    void Start();
    seastar::future<> stop();

    // Distributed version of the class
    typedef seastar::distributed<RPCDispatcher> Dist_t;
public:
    // Registration interface

    void RegisterMessageHandler(Verb verb, MessageHandler_t handler);

private:
    // This distributed container will allow us to access the listeners associated with this dispatcher instance
    ListenerFactory::Dist_t &_listeners;

    // the message handlers
    std::unordered_map<Verb, MessageHandler_t> _handlers;
};

} // namespace k2tx
