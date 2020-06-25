/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

// stl
#include <functional>
#include <unordered_map>
#include <exception>

// third party
#include <seastar/core/distributed.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/reference_wrapper.hh> // for seastar::ref

// k2
#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include "RPCProtocolFactory.h"
#include "Request.h"
#include "Status.h"

namespace k2 {

// Utility function which helps RPC users create responses without spelling out nested template arguments
template <typename T>
inline seastar::future<std::tuple<Status, T>> RPCResponse(Status&& s, T&& r) {
    return seastar::make_ready_future<std::tuple<Status, T>>(std::tuple<Status, T>(std::forward<Status>(s), std::forward<T>(r)));
}

// An RPC dispatcher is the interaction point between a service application and underlying transport.
// It dispatches incoming RPC messages to message observers, and provides RPC channels for sending
// outgoing messages.
// This class should be used as a distributed<> container
class RPCDispatcher: public seastar::weakly_referencable<RPCDispatcher> {
public: // types
    // distributed<> version of the class
    typedef seastar::distributed<RPCDispatcher> Dist_t;
    
    // thrown when you attempt to register something more than once
    struct DuplicateRegistrationException : public std::exception {
        virtual const char* what() const noexcept override{ return "duplicate registration not allowed";}
    };
    struct SystemVerbRegistrationNotAllowedException : public std::exception {
        virtual const char* what() const noexcept override{ return "system verb registration not allowed";}
    };

    // deliverd to promises when dispatcher is shutting down
    struct DispatcherShutdown : public std::exception {
        virtual const char* what() const noexcept override{ return "dispatcher has shut down"; }
    };

    // we use this exception to signal that a protocol isn't supported (e.g. when attempting to send)
    struct UnsupportedProtocolException : public std::exception {
        virtual const char* what() const noexcept override{ return "protocol not supported";}
    };

    // we use this to resolve promises for replies in the sendRequest call
    struct RequestTimeoutException : public std::exception {
        virtual const char* what() const noexcept override{ return "request timed out";}
    };

public:
    // Construct an RPC dispatcher
    RPCDispatcher();

    // destructor
    ~RPCDispatcher();

public: // distributed<> interface
    // iface: called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    seastar::future<> stop();

    // Should be called by user when all distributed objects have been created
    void start();

public: // message-oriented API
    // This method is used to register protocols with the dispatcher.
    // we don't allow replacing providers for protocols. If a provider already exists, a
    // DuplicateRegistrationException exception will be raised
    // this method is normally called via distributed<>::invoke_on_all
    seastar::future<> registerProtocol(seastar::reference_wrapper<RPCProtocolFactory::Dist_t> protocol);

    // registerMessageObserver allows you to register an observer function for a given RPC verb.
    // You can have at most one observer per verb. a DuplicateRegistrationException will be
    // thrown if there is an observer already installed for this verb
    void registerMessageObserver(Verb verb, RequestObserver_t observer);

    // registerLowTransportMemoryObserver allows the user to register an observer which will be called when
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
    void registerLowTransportMemoryObserver(LowTransportMemoryObserver_t observer);

    // This method creates an endpoint for a given URL. The endpoint is needed in order to
    // 1. obtain protocol-specific payloads
    // 2. send messages.
    // returns blank pointer if we failed to parse the url or if the protocol is not supported
    std::unique_ptr<TXEndpoint> getTXEndpoint(String url);

    // Returns the listener endpoint for the given protocol (or empty pointer if not supported)
    seastar::lw_shared_ptr<TXEndpoint> getServerEndpoint(const String& protocol);

    //  List all server endpoint supported by this dispatcher
    std::vector<seastar::lw_shared_ptr<TXEndpoint>> getServerEndpoints() const;

    // Invokes the remote rpc for the given verb with the given payload. This is an asynchronous API. No guarantees
    // are made on the delivery of the payload after the call returns.
    // This is a lower-level API which is useful for sending messages that do not expect replies.
    seastar::future<> send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint);

    // Invokes the remote rpc for the given verb with the given payload. This is an asynchronous API. No guarantees
    // are made on the delivery of the payload.
    // This API is provided to allow users to send requests which expect replies (as opposed to send() above).
    // The method provides a future<> based callback support via the return value.
    // The future will complete with exception if the given timeout is reached before we receive a response.
    // if we receive a response after the timeout is reached, we will ignore it internally.
    seastar::future<std::unique_ptr<Payload>>
    sendRequest(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, Duration timeout);

    // Use this method to reply to a given Request, with the given payload. This method should be normally used
    // in message observers to respond to clients.
    seastar::future<> sendReply(std::unique_ptr<Payload> payload, Request& forRequest);
    
    seastar::future<> setAddressCore(std::pair<String, int> url_core);

    

public: // RPC-oriented interface. Small convenience so that users don't have to deal with Payloads directly
    // Same as sendRequest but for RPC types, not raw payloads
    template<class Request_t, class Response_t>
    seastar::future<std::tuple<Status, Response_t>> callRPC(Verb verb, Request_t& request, TXEndpoint& endpoint, Duration timeout) {
        auto payload = endpoint.newPayload();
        payload->write(request);
        K2DEBUG("RPC Request call to endpoint: " << endpoint.getURL());

        return sendRequest(verb, std::move(payload), endpoint, timeout)
            .then([](std::unique_ptr<Payload>&& responsePayload) {
                // parse status
                auto result = std::make_tuple<Status, Response_t>(Status(), Response_t());
                if (!responsePayload->read(std::get<0>(result))) {
                    std::get<0>(result) = Statuses::S500_Internal_Server_Error("unable to parse status from response");
                }
                else {
                    if (!responsePayload->read(std::get<1>(result))) {
                        // failed to parse a Response_t
                        std::get<0>(result) = Statuses::S500_Internal_Server_Error("unable to parse response object");
                    }
                }
                return result;
            })
            .handle_exception([](auto exc) {
                try {
                    std::rethrow_exception(exc);
                }
                catch (const RPCDispatcher::RequestTimeoutException&) {
                    return std::make_tuple<Status, Response_t>(Statuses::S503_Service_Unavailable("client timed out"), Response_t());
                }
                catch (const std::exception &e) {
                    K2ERROR("RPC send failed with uncaught exception: " << e.what());
                }
                catch (...) {
                    K2ERROR("RPC send failed with unknown exception");
                }

                return std::make_tuple<Status, Response_t>(Statuses::S500_Internal_Server_Error("unknown exception while sending request"), Response_t());
            });
    }

    // Register a handler for requests of type Request_t. You are required to respond with an object of type Response_t
    // and a Status for your request
    template <class Request_t, class Response_t>
    void registerRPCObserver(Verb verb, RPCRequestObserver_t<Request_t, Response_t> observer) {
        // wrap the RPC observer into a message observer
        registerMessageObserver(verb, [this, observer=std::move(observer)](Request&& request) mutable {
            // we're ignoring the returned future here so we can't wait for it before the rpc dispatcher exits
            // to guard against segv on shutdown, obtain a weak pointer
            (void)seastar::do_with(std::move(request), Request_t{}, weak_from_this(),
                [&observer](auto& request, auto& rpcRequest, auto& disp) {
                    if (!disp) return seastar::make_ready_future();

                    if (!request.payload->read(rpcRequest)) {
                        auto reply = request.endpoint.newPayload();
                        reply->write(Statuses::S400_Bad_Request("unable to parse incoming request"));
                        return disp->sendReply(std::move(reply), request);
                    }
                    // if disp was still alive, it's safe to call observer
                    return observer(std::move(rpcRequest))
                        .then([&](auto&& result) mutable {
                            if (!disp) {
                                K2WARN("dispatcher is going down: unable to send response to " << request.endpoint.getURL());
                                return seastar::make_ready_future();
                            }

                            auto& [status, response] = result;
                            // write out the status first
                            auto reply = request.endpoint.newPayload();
                            reply->write(status);
                            // write out the Response_t
                            reply->write(response);
                            return disp->sendReply(std::move(reply), request);
                        })
                        .handle_exception([&](auto exc) mutable {
                            K2ERROR_EXC("RPC handler failed with uncaught exception", exc);
                            if (disp) {
                                auto reply = request.endpoint.newPayload();
                                reply->write(Statuses::S500_Internal_Server_Error("server caught exception processing request"));
                                reply->write(Response_t{});
                                return disp->sendReply(std::move(reply), request);
                            }
                            return seastar::make_ready_future();
                        });
               });
        });
    }

private:  // methods
    // Process new messages received from protocols
    void _handleNewMessage(Request&& request);

    // Helper method useds to send messages
    seastar::future<> _send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata meta);

private: // fields
    // the protocols this dispatcher will be able to support
    std::unordered_map<String, seastar::shared_ptr<IRPCProtocol>> _protocols;

    // the mapping we use to discover whether the given URL is owned by the other cores in the same process in order to perform in process loopback
    std::unordered_map<String, int> _url_cores;

    // the message observers
    std::unordered_map<Verb, RequestObserver_t> _observers;

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

    ConfigVar<bool> _txUseCrossCoreLoopback{"tx_xcore_loopback", true};
};

// global RPC dist container which can be initialized by main() of an application so that
// all users of RPC can just use this global to access RPC capabilities
extern RPCDispatcher::Dist_t ___RPC___;
inline RPCDispatcher& RPC() { return ___RPC___.local(); }
inline RPCDispatcher::Dist_t& RPCDist() { return ___RPC___; }
} // namespace k2
