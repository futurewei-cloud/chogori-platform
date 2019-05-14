//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

// stl
#include <functional>
#include <unordered_map>
#include <exception>
#include <chrono>

// third party
#include <seastar/core/distributed.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/reference_wrapper.hh> // for seastar::ref

// k2
#include "RPCProtocolFactory.h"
#include "common/Common.h"
#include "Request.h"

namespace k2 {

// An RPC dispatcher is the interaction point between a service application and underlying transport.
// It dispatches incoming RPC messages to message observers, and provides RPC channels for sending
// outgoing messages.
// This class should be used as a distributed<> container
class RPCDispatcher: public seastar::weakly_referencable<RPCDispatcher> {
public: // types
    // The type of a message observer
    // TODO See if we can use something faster than std::function.
    // Benchmark indicates 20ns penalty per runtime call
    // See https://www.boost.org/doc/libs/1_69_0/doc/html/function/faq.html
    typedef std::function<void(Request& request)> MessageObserver_t;

    // the type of a low memory observer. This function will be called when a transport requires a release of
    // some memory
    typedef std::function<void(const String& ttype, size_t requiredBytes)> LowTransportMemoryObserver_t;

    // distributed<> version of the class
    typedef seastar::distributed<RPCDispatcher> Dist_t;

    // thrown when you attempt to register something more than once
    class DuplicateRegistrationException : public std::exception {};

    // deliverd to promises when dispatcher is shutting down
    class DispatcherShutdown : public std::exception {};

    // we use this exception to signal that a protocol isn't supported (e.g. when attempting to send)
    class UnsupportedProtocolException : public std::exception {};

    // we use this to resolve promises for replies in the SendRequest call
    class RequestTimeoutException : public std::exception {};

public:
    // Construct an RPC dispatcher
    RPCDispatcher();

    // destructor
    ~RPCDispatcher();

public: // distributed<> interface
    // iface: called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    seastar::future<> stop();

    // Should be called by user when all distributed objects have been created
    void Start();

public: // API
    // This method is used to register protocols with the dispatcher.
    // we don't allow replacing providers for protocols. If a provider already exists, a
    // DuplicateRegistrationException exception will be raised
    // this method is normally called via distributed<>::invoke_on_all
    seastar::future<> RegisterProtocol(seastar::reference_wrapper<RPCProtocolFactory::Dist_t> protocol);

    // RegisterMessageObserver allows you to register an observer function for a given RPC verb.
    // You can have at most one observer per verb. a DuplicateRegistrationException will be
    // thrown if there is an observer already installed for this verb
    void RegisterMessageObserver(Verb verb, MessageObserver_t observer);

    // RegisterLowTransportMemoryObserver allows the user to register an observer which will be called when
    // a transport becomes low on memory.
    // The call is triggered every time a transport has to perform allocation of its buffers, and
    // advises the user which transport type requires release of buffers, and what is the required total number
    // of bytes that should be released.
    //
    // The intended use case here is for applications which hold on to Payloads for long periods of time.
    // These applications should register themselves here, and when called should release enough Payloads to satisfy
    // the requiredNumberOfBytes parameter in their callback.
    // Since we're dealing with multiple transports, the callback also indicates which transport protocol
    // required release. The user can then release Payloads whose transport protocol matches.
    void RegisterLowTransportMemoryObserver(LowTransportMemoryObserver_t observer);

    // This method creates an endpoint for a given URL. The endpoint is needed in order to
    // 1. obtain protocol-specific payloads
    // 2. send messages.
    // returns blank pointer if we failed to parse the url or if the protocol is not supported
    std::unique_ptr<TXEndpoint> GetTXEndpoint(String url);

    // Invokes the remote rpc for the given verb with the given payload. This is an asyncronous API. No guarantees
    // are made on the delivery of the payload after the call returns.
    // This is a lower-level API which is useful for sending messages that do not expect replies.
    void Send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint);

    // Invokes the remote rpc for the given verb with the given payload. This is an asyncronous API. No guarantees
    // are made on the delivery of the payload.
    // This API is provided to allow users to send requests which expect replies (as opposed to Send() above).
    // The method provides a future<> based callback support via the return value.
    // The future will complete with exception if the given timeout is reached before we receive a response.
    // if we receive a response after the timeout is reached, we will ignore it internally.
    seastar::future<std::unique_ptr<Payload>>
    SendRequest(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, Duration timeout);

    // Use this method to reply to a given Request, with the given payload. This method should be normally used
    // in message observers to respond to clients.
    void SendReply(std::unique_ptr<Payload> payload, Request& forRequest);

private: // methods
    // Process new messages received from protocols
    void _handleNewMessage(Request& request);

    // Helper method useds to send messages
    void _send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata meta);

private: // fields
    // the protocols this dispatcher will be able to support
    std::unordered_map<String, seastar::shared_ptr<IRPCProtocol>> _protocols;

    // the message observers
    std::unordered_map<Verb, MessageObserver_t> _observers;

    // to track the request-reply promises and timeouts
    typedef seastar::promise<std::unique_ptr<Payload>> PayloadPromise;
    struct ResponseTracker {
        PayloadPromise promise;
        seastar::timer<> timer;
    };

    // map of all pending request-reply
    std::unordered_map<uint64_t, ResponseTracker> _rrPromises;

    // our observer for low memory events
    LowTransportMemoryObserver_t _lowMemObserver;

    // sequence id used for request-reply
    // TODO use something a bit stronger than simple increment integer
    uint32_t _msgSequenceID;

private: // don't need
    RPCDispatcher(const RPCDispatcher& o) = delete;
    RPCDispatcher(RPCDispatcher&& o) = delete;
    RPCDispatcher& operator=(const RPCDispatcher& o) = delete;
    RPCDispatcher& operator=(RPCDispatcher&& o) = delete;
};

} // namespace k2
