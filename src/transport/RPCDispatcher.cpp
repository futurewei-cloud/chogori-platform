//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <cstdlib>

#include "RPCDispatcher.h"
#include "TXEndpoint.h"
#include "common/Log.h"

namespace k2{

RPCDispatcher::Dist_t RPC;

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
        K2DEBUG("handling request for verb="<< request.verb <<", from ep="<< request.endpoint.getURL());
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

    return seastar::make_ready_future<>();
}

void RPCDispatcher::registerMessageObserver(Verb verb, RequestObserver_t observer) {
    K2DEBUG("registering message observer for verb: " << verb);
    if (observer == nullptr) {
        _observers.erase(verb);
        K2DEBUG("Removing message observer for verb: " << verb);
        return;
    }
    if (verb == KnownVerbs::ZEROVERB) {
        // can't allow registration of the ZEROVERB
        throw DuplicateRegistrationException();
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
    K2DEBUG("handling request for verb="<< request.verb <<", from ep="<< request.endpoint.getURL());
    // see if this is a response
    if (request.metadata.isResponseIDSet()) {
        // process as a response
        auto nodei = _rrPromises.find(request.metadata.responseID);
        if (nodei == _rrPromises.end()) {
            K2DEBUG("no handler for response for msgid: " << request.metadata.responseID )
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
        K2DEBUG("Dispatching request for verb="<< request.verb <<", from ep="<< request.endpoint.getURL());
        iter->second(std::move(request));
    }
    else {
        K2DEBUG("no observer for verb " << request.verb << ", from " << request.endpoint.getURL());
    }
}

void RPCDispatcher::_send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, MessageMetadata meta) {
    K2DEBUG("sending message for verb: " << verb << ", to endpoint=" << endpoint.getURL());

    auto protoi = _protocols.find(endpoint.getProtocol());
    if (protoi == _protocols.end()) {
        K2WARN("Unsupported protocol: "<< endpoint.getProtocol());
        return;
    }
    protoi->second->send(verb, std::move(payload), endpoint, std::move(meta));
}

void RPCDispatcher::send(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint) {
    K2DEBUG("Plain send");
    MessageMetadata metadata;
    _send(verb, std::move(payload), endpoint, std::move(metadata));
}

void RPCDispatcher::sendReply(std::unique_ptr<Payload> payload, Request& forRequest) {
    K2DEBUG("Reply send for request: " << forRequest.metadata.requestID);
    MessageMetadata metadata = MessageMetadata::createResponse(forRequest.metadata);
    _send(KnownVerbs::ZEROVERB, std::move(payload), forRequest.endpoint, std::move(metadata));
}

seastar::future<std::unique_ptr<Payload>>
RPCDispatcher::sendRequest(Verb verb, std::unique_ptr<Payload> payload, TXEndpoint& endpoint, Duration timeout) {
    uint64_t msgid = _msgSequenceID++;
    K2DEBUG("Request send with msgid=" << msgid);

    // the promise gets fulfilled when prom for this msgid comes back.
    PayloadPromise prom;
    // record the promise so that we can fulfil it if we get a response
    MessageMetadata metadata;
    metadata.setRequestID(msgid);

    _send(verb, std::move(payload), endpoint, std::move(metadata));

    seastar::timer<> timer([this, msgid] {
        // raise an exception in the promise for this request.
        K2DEBUG("send request timed out for msgid=" << msgid);
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
    for(const auto& kvp : _protocols)
        result.push_back(kvp.second->getServerEndpoint());
    return result;
}

}// namespace k2
