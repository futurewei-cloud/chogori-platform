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

#include <cstdlib>
#include <seastar/core/sleep.hh>

#include <k2/common/Log.h>
#include "RPCDispatcher.h"
#include "TXEndpoint.h"

namespace k2{

RPCDispatcher::RPCDispatcher() : _msgSequenceID(uint32_t(std::rand())) {
    K2LOG_D(log::tx, "ctor");
    registerLowTransportMemoryObserver(nullptr);
}

RPCDispatcher::~RPCDispatcher() {
    K2LOG_D(log::tx, "dtor");
}

seastar::future<>
RPCDispatcher::registerProtocol(seastar::reference_wrapper<RPCProtocolFactory::Dist_t> dprotocol) {
    // we don't allow replacing providers for protocols. Raise an exception if there is a provider already
    auto proto = dprotocol.get().local().instance();
    K2LOG_D(log::tx, "registering protocol: {}", proto->supportedProtocol());
    auto emplace_pair = _protocols.try_emplace(proto->supportedProtocol(), proto);
    if (!emplace_pair.second) {
        throw DuplicateRegistrationException();
    }

    // set ourselves to handle all messages from this protocol
    // weird gymnastics to get around the fact that weak pointers aren't copyable
    proto->setMessageObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (Request&& request) {
        K2LOG_D(log::tx, "handling request for verb={}, from ep={}", int(request.verb), request.endpoint.url);
        seastar::weak_ptr<RPCDispatcher>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP) {
            weakP->_handleNewMessage(std::move(request));
        }
    });

    // set ourselves as the low-memory observer for the protocol
    proto->setLowTransportMemoryObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (const String& protoname, size_t requiredBytes) {
        K2LOG_D(log::tx, "lowmem notification from proto={}, requiredBytes={}", protoname, requiredBytes);
        seastar::weak_ptr<RPCDispatcher>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP) {
            weakP->_lowMemObserver(protoname, requiredBytes);
        }
    });

    auto serverep = proto->getServerEndpoint();
    if (serverep){
        std::pair<String, int> url_core;
        url_core = std::make_pair(serverep->url, seastar::this_shard_id());
        K2LOG_D(log::tx, "BroadCast URL and Core ID {}", serverep->url);
        return RPCDist().invoke_on_all([url_core](RPCDispatcher& disp) {
            return disp.setAddressCore(url_core);
        });
    }
    return seastar::make_ready_future<>();
}

void RPCDispatcher::registerMessageObserver(Verb verb, RequestObserver_t observer) {
    K2LOG_D(log::tx, "registering message observer for verb: {}", int(verb));
    if (observer == nullptr) {
        _observers.erase(verb);
        K2LOG_D(log::tx, "Removing message observer for verb: {}", int(verb));
        return;
    }
    if (verb >= InternalVerbs::MAX_VERB) {
        // can't allow registration of the NIL verb
        throw SystemVerbRegistrationNotAllowedException();
    }
    // we don't allow replacing verb observers. Raise an exception if there is an observer already
    auto emplace_pair = _observers.try_emplace(verb, observer);
    if (!emplace_pair.second) {
        throw DuplicateRegistrationException();
    }
}

void RPCDispatcher::start() {
    K2LOG_D(log::tx, "start");
}

seastar::future<> RPCDispatcher::stop() {
    K2LOG_D(log::tx, "stop");

    // reset the messsage observer for each protocol as we're about to go away
    for(auto&& proto: _protocols) {
        proto.second->setMessageObserver(nullptr);
    }
    _protocols.clear();

    // complete all promises
    for(auto&& promise: _rrPromises) {
        promise.second.promise.set_exception(DispatcherShutdown());
    }
    _rrPromises.clear();
    return seastar::make_ready_future<>();
}

// Process new messages received from protocols
void RPCDispatcher::_handleNewMessage(Request&& request) {
    K2LOG_D(log::tx, "handling request for verb={}, from ep={}", int(request.verb), request.endpoint.url);
    // see if this is a response
    if (request.metadata.isResponseIDSet()) {
        // process as a response
        auto nodei = _rrPromises.find(request.metadata.responseID);
        if (nodei == _rrPromises.end()) {
            K2LOG_D(log::tx, "no handler for response for msgid: {}", request.metadata.responseID )
            // TODO emit metric for RR without msid
            return;
        }
        // we have a response
        nodei->second.timer.cancel();
        nodei->second.promise.set_value(std::move(request.payload));
        _rrPromises.erase(nodei);
        return;
    }
    auto iter = _observers.find(request.verb);
    if (iter != _observers.end()) {
        K2LOG_D(log::tx, "Dispatching request for verb={}, from ep={}", int(request.verb), request.endpoint.url);
        // TODO emit verb-dimension metric for duration of handling
        try {
            iter->second(std::move(request));
        } catch (std::exception& exc) {
            K2LOG_E(log::tx, "Caught exception while dispatching request: {}", exc.what());
        } catch (...) {
            K2LOG_E(log::tx, "Caught unknown exception while dispatching request");
        }
    }
    else {
        K2LOG_D(log::tx, "no observer for verb {}, from {}", request.verb, request.endpoint.url);
        // TODO emit metric
    }
}


seastar::future<>
RPCDispatcher::_send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata meta) {
    auto ep = RPC().getServerEndpoint(endpoint.protocol);
    auto protoi = _protocols.find(endpoint.protocol);
    if (protoi == _protocols.end()) {
        K2LOG_W(log::tx, "Unsupported protocol: {}", endpoint.protocol);
        return seastar::make_ready_future<>();
    }
    auto serverep = protoi->second->getServerEndpoint();
    K2LOG_D(log::tx,
        "sending message for verb={}, to endpoint={}, with server endpoint={}, payload size={}, payload capacity={}",
        int(verb), endpoint.url, (serverep ? serverep->url : String("none")),
        (payload ? payload->getSize() : 0), (payload ? payload->getCapacity() : 0));
    if (_txUseCrossCoreLoopback()) {
        auto core = _url_cores.find(endpoint.url);
        if (core != _url_cores.end()){
            // rewind the payload to the correct position
            payload->seek(txconstants::MAX_HEADER_SIZE);
            meta.setPayloadSize(payload->getDataRemaining());

            if (serverep && endpoint == *serverep){
                _handleNewMessage(Request(verb, *RPC().getServerEndpoint(endpoint.protocol), std::move(meta), std::move(payload)));
            }
            else{
                //We don't care about the result of this call since we don't make a promise that we're going to deliver the data.
                (void) RPCDist().invoke_on(core->second, &k2::RPCDispatcher::_handleNewMessage, Request(verb, *RPC().getServerEndpoint(endpoint.protocol), std::move(meta), std::move(payload))).
                handle_exception([&](auto exc) mutable {
                    K2LOG_W_EXC(log::tx, exc, "invoke_on failed");
                    return seastar::make_ready_future();
                });
            }
            return seastar::make_ready_future<>();
        }
    }
    protoi->second->send(verb, std::move(payload), endpoint, std::move(meta));
    return seastar::make_ready_future<>();
}


seastar::future<>
RPCDispatcher::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint) {
    MessageMetadata metadata;
    return _send(verb, std::move(payload), endpoint, std::move(metadata));
}

seastar::future<>
RPCDispatcher::setAddressCore(std::pair<String, int> url_core) {
    _url_cores.insert(std::move(url_core));
    return seastar::make_ready_future<>();
}

seastar::future<>
RPCDispatcher::sendReply(std::unique_ptr<Payload> payload, Request& forRequest) {
    MessageMetadata metadata;
    metadata.setResponseID(forRequest.metadata.requestID);
    return _send(InternalVerbs::NIL, std::move(payload), forRequest.endpoint, std::move(metadata));
}

seastar::future<std::unique_ptr<Payload>>
RPCDispatcher::sendRequest(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, Duration timeout) {
    uint64_t msgid = _msgSequenceID++;
    K2LOG_D(log::tx, "Request send with msgid={}, timeout={}, ep={}", msgid, timeout, endpoint.url);

    // the promise gets fulfilled when prom for this msgid comes back.
    PayloadPromise prom;
    // record the promise so that we can fulfil it if we get a response
    MessageMetadata metadata;
    metadata.setRequestID(msgid);

    seastar::timer<> timer([this, msgid] {
        // raise an exception in the promise for this request.
        K2LOG_D(log::tx, "send request timed out for msgid={}", msgid);
        // TODO emit metric for timeout
        auto iter = this->_rrPromises.find(msgid);
        K2ASSERT(log::tx, iter != this->_rrPromises.end(), "unable to find promise for timer");
        iter->second.promise.set_exception(RequestTimeoutException());
        this->_rrPromises.erase(iter);
    });
    timer.arm(timeout);

    auto fut = prom.get_future();
    _rrPromises.emplace(msgid, ResponseTracker{std::move(prom), std::move(timer)});

    return _send(verb, std::move(payload), endpoint, std::move(metadata)).
    then([fut=std::move(fut)] () mutable {
        return std::move(fut);
    });


}

void RPCDispatcher::registerLowTransportMemoryObserver(LowTransportMemoryObserver_t observer) {
    K2LOG_D(log::tx, "register low mem observer");
    if (observer == nullptr) {
        K2LOG_D(log::tx, "setting default low transport memory observer");
        _lowMemObserver = [](const String& ttype, size_t suggestedBytes) {
            K2LOG_W(log::tx, "no low-mem observer installed. Transport: {}, requires release of {} bytes", ttype, suggestedBytes);
        };
    }
    else {
        _lowMemObserver = observer;
    }
}

std::unique_ptr<TXEndpoint> RPCDispatcher::getTXEndpoint(String url) {
    K2LOG_D(log::tx, "get endpoint for {}", url)
    // temporary endpoint just so that we can see what the protocol is supposed to be
    auto ep = TXEndpoint::fromURL(url, nullptr);
    if (!ep) {
        K2LOG_W(log::tx, "Unable to get tx endpoint for url: {}", url);
        return nullptr;
    }

    auto protoi = _protocols.find(ep->protocol);
    if (protoi == _protocols.end()) {
        K2LOG_W(log::tx, "Unsupported protocol: {}", ep->protocol);
        return nullptr;
    }
    return protoi->second->getTXEndpoint(std::move(url));
}

seastar::lw_shared_ptr<TXEndpoint> RPCDispatcher::getServerEndpoint(const String& protocol) {
    auto protoi = _protocols.find(protocol);
    if (protoi == _protocols.end()) {
        K2LOG_W(log::tx, "Unsupported protocol: {}", protocol);
        return nullptr;
    }
    return protoi->second->getServerEndpoint();
}

std::vector<seastar::lw_shared_ptr<TXEndpoint>> RPCDispatcher::getServerEndpoints() const {
    std::vector<seastar::lw_shared_ptr<TXEndpoint>> result;
    for(const auto& kvp : _protocols) {
        auto ep = kvp.second->getServerEndpoint();
        if (ep) {
            result.push_back(kvp.second->getServerEndpoint());
        }
    }
    return result;
}

}// namespace k2
