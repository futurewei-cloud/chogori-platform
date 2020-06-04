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

RPCDispatcher::Dist_t ___RPC___;

RPCDispatcher::RPCDispatcher() : _msgSequenceID(uint32_t(std::rand())) {
    K2DEBUG("ctor");
    registerLowTransportMemoryObserver(nullptr);
}

RPCDispatcher::~RPCDispatcher() {
    K2DEBUG("dtor");
}

seastar::future<>
RPCDispatcher::registerProtocol(seastar::reference_wrapper<RPCProtocolFactory::Dist_t> dprotocol) {
    // we don't allow replacing providers for protocols. Raise an exception if there is a provider already
    auto proto = dprotocol.get().local().instance();
    K2DEBUG("registering protocol: " << proto->supportedProtocol());
    auto emplace_pair = _protocols.try_emplace(proto->supportedProtocol(), proto);
    if (!emplace_pair.second) {
        throw DuplicateRegistrationException();
    }

    // set ourselves to handle all messages from this protocol
    // weird gymnastics to get around the fact that weak pointers aren't copyable
    proto->setMessageObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (Request&& request) {
        K2DEBUG("handling request for verb="<< int(request.verb) <<", from ep="<< request.endpoint.getURL());
        seastar::weak_ptr<RPCDispatcher>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP) {
            weakP->_handleNewMessage(std::move(request));
        }
    });

    // set ourselves as the low-memory observer for the protocol
    proto->setLowTransportMemoryObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (const String& protoname, size_t requiredBytes) {
        K2DEBUG("lowmem notification from proto="<< protoname <<", requiredBytes="<< requiredBytes);
        seastar::weak_ptr<RPCDispatcher>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP) {
            weakP->_lowMemObserver(protoname, requiredBytes);
        }
    });

    auto ep = RPC().getServerEndpoint(proto->supportedProtocol());
    if (ep){
        std::pair<String, int> url_core;
        url_core = std::make_pair(ep->getURL(), seastar::engine().cpu_id());
        K2INFO("BroadCast URL and Core ID" << ep->getURL());
        return RPCDist().invoke_on_all(&k2::RPCDispatcher::setAddressCore, url_core);
    }
    return seastar::make_ready_future<>();
}

void RPCDispatcher::registerMessageObserver(Verb verb, RequestObserver_t observer) {
    K2DEBUG("registering message observer for verb: " << int(verb));
    if (observer == nullptr) {
        _observers.erase(verb);
        K2DEBUG("Removing message observer for verb: " << int(verb));
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
    K2DEBUG("start");
}

seastar::future<> RPCDispatcher::stop() {
    K2DEBUG("stop");

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
    K2DEBUG("handling request for verb="<< int(request.verb) <<", from ep="<< request.endpoint.getURL());
    // see if this is a response
    if (request.metadata.isResponseIDSet()) {
        // process as a response
        auto nodei = _rrPromises.find(request.metadata.responseID);
        if (nodei == _rrPromises.end()) {
            K2DEBUG("no handler for response for msgid: " << request.metadata.responseID )
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
        K2DEBUG("Dispatching request for verb="<< int(request.verb) <<", from ep="<< request.endpoint.getURL());
        // TODO emit verb-dimension metric for duration of handling
        try {
            iter->second(std::move(request));
        } catch (std::exception& exc) {
            K2ERROR("Caught exception while dispatching request: " << exc.what());
        } catch (...) {
            K2ERROR("Caught unknown exception while dispatching request");
        }
    }
    else {
        K2DEBUG("no observer for verb " << request.verb << ", from " << request.endpoint.getURL());
        // TODO emit metric
    }
}


void RPCDispatcher::_send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata meta) {
    auto ep = RPC().getServerEndpoint(endpoint.getProtocol());
    //K2INFO("From: " << ep->getURL() <<" To " << endpoint.getURL());
    auto protoi = _protocols.find(endpoint.getProtocol());
    if (protoi == _protocols.end()) {
        K2WARN("Unsupported protocol: "<< endpoint.getProtocol());
        return;
    }
    auto serverep = protoi->second->getServerEndpoint();
    K2DEBUG("sending message for verb: " << int(verb) << ", to endpoint=" << endpoint.getURL()
            << ", with server endpoint: " << (serverep ? serverep->getURL() : String("none"))
            << ", payload size=" << (payload?payload->getSize():0)
            << ", payload capacity=" << (payload?payload->getCapacity():0));
    if (serverep && endpoint == *serverep) {
        // deliver via a future to possibly yield if there is a loop of send/receive requests
        (void) seastar::sleep(0ns)
            .then([disp=weak_from_this(), verb, endpoint, meta=std::move(meta), payload=std::move(payload)] () mutable {
                if (disp) {
                    // rewind the payload to the correct position
                    payload->seek(txconstants::MAX_HEADER_SIZE);
                    meta.setPayloadSize(payload->getDataRemaining());
                    disp->_handleNewMessage(Request(verb, endpoint, std::move(meta), std::move(payload)));
                }
        });
        return;
    }

    auto core = _url_cores.find(endpoint.getURL());
    if (core != _url_cores.end()){
        (void) seastar::sleep(0ns)
            .then([core, verb, endpoint, meta=std::move(meta), payload=std::move(payload)] () mutable {
                // rewind the payload to the correct position
                payload->seek(txconstants::MAX_HEADER_SIZE);
                meta.setPayloadSize(payload->getDataRemaining());
                auto return_value = RPCDist().invoke_on(core->second, &k2::RPCDispatcher::_handleNewMessage, Request(verb, *RPC().getServerEndpoint(endpoint.getProtocol()), std::move(meta), std::move(payload)));
        });
        return;
    }

    protoi->second->send(verb, std::move(payload), endpoint, std::move(meta));
}


void RPCDispatcher::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint) {
    K2DEBUG("Plain send");
    MessageMetadata metadata;
    _send(verb, std::move(payload), endpoint, std::move(metadata));
}

seastar::future<>
RPCDispatcher::setAddressCore(std::pair<String, int> url_core) {
    auto url = url_core.first;
    int core_id = url_core.second;
    K2DEBUG("Setting address core: " << url << ", for id=" << core_id);
    _url_cores.insert(std::make_pair(url, core_id));
    return seastar::make_ready_future<>();
}

void RPCDispatcher::sendReply(std::unique_ptr<Payload> payload, Request& forRequest) {
    MessageMetadata metadata;
    metadata.setResponseID(forRequest.metadata.requestID);
    _send(InternalVerbs::NIL, std::move(payload), forRequest.endpoint, std::move(metadata));
}

seastar::future<std::unique_ptr<Payload>>
RPCDispatcher::sendRequest(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, Duration timeout) {
    uint64_t msgid = _msgSequenceID++;
    K2DEBUG("Request send with msgid=" << msgid << ", timeout=" << timeout << ", ep=" << endpoint.getURL());

    // the promise gets fulfilled when prom for this msgid comes back.
    PayloadPromise prom;
    // record the promise so that we can fulfil it if we get a response
    MessageMetadata metadata;
    metadata.setRequestID(msgid);

    _send(verb, std::move(payload), endpoint, std::move(metadata));

    seastar::timer<> timer([this, msgid] {
        // raise an exception in the promise for this request.
        K2DEBUG("send request timed out for msgid=" << msgid);
        // TODO emit metric for timeout
        auto iter = this->_rrPromises.find(msgid);
        assert(iter != this->_rrPromises.end());
        iter->second.promise.set_exception(RequestTimeoutException());
        this->_rrPromises.erase(iter);
    });
    timer.arm(timeout);

    auto fut = prom.get_future();
    _rrPromises.emplace(msgid, ResponseTracker{std::move(prom), std::move(timer)});

    return fut;
}

void RPCDispatcher::registerLowTransportMemoryObserver(LowTransportMemoryObserver_t observer) {
    K2DEBUG("register low mem observer");
    if (observer == nullptr) {
        K2DEBUG("setting default low transport memory observer");
        _lowMemObserver = [](const String& ttype, size_t suggestedBytes) {
            K2WARN("no low-mem observer installed. Transport: "<< ttype << ", requires release of "<< suggestedBytes << "bytes");
        };
    }
    else {
        _lowMemObserver = observer;
    }
}

std::unique_ptr<TXEndpoint> RPCDispatcher::getTXEndpoint(String url) {
    K2DEBUG("get endpoint for " << url)
    // temporary endpoint just so that we can see what the protocol is supposed to be
    auto ep = TXEndpoint::fromURL(url, nullptr);
    if (!ep) {
        K2WARN("Unable to get tx endpoint for url: " << url);
        return nullptr;
    }

    auto protoi = _protocols.find(ep->getProtocol());
    if (protoi == _protocols.end()) {
        K2WARN("Unsupported protocol: "<< ep->getProtocol());
        return nullptr;
    }
    return protoi->second->getTXEndpoint(std::move(url));
}

seastar::lw_shared_ptr<TXEndpoint> RPCDispatcher::getServerEndpoint(const String& protocol) {
    auto protoi = _protocols.find(protocol);
    if (protoi == _protocols.end()) {
        K2WARN("Unsupported protocol: "<< protocol);
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
