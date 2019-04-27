//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <cstdlib>

#include "RPCDispatcher.h"
#include "Endpoint.h"
#include "Log.h"

namespace k2tx{

RPCDispatcher::RPCDispatcher():
    _msgSequenceID(uint32_t(std::rand())) {
    K2DEBUG("ctor");
    RegisterLowTransportMemoryObserver(nullptr);
}

RPCDispatcher::~RPCDispatcher() {
    K2DEBUG("dtor");
}

seastar::future<>
RPCDispatcher::RegisterProtocol(seastar::reference_wrapper<RPCProtocolFactory::Dist_t> dprotocol) {
    // we don't allow replacing providers for protocols. Raise an exception if there is a provider already
    auto proto = dprotocol.get().local().instance();
    K2DEBUG("registering protocol: " << proto->SupportedProtocol());
    auto emplace_pair = _protocols.try_emplace(proto->SupportedProtocol(), proto);
    if (!emplace_pair.second) {
        throw DuplicateRegistrationException();
    }

    // set ourselves to handle all messages from this protocol
    // weird gymnastics to get around the fact that weak pointers aren't copyable
    proto->SetMessageObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (Request& request) {
        K2DEBUG("handling request for verb="<< request.verb <<", from ep="<< request.endpoint.GetURL());
        seastar::weak_ptr<RPCDispatcher>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP) {
            weakP->_handleNewMessage(request);
        }
    });

    // set ourselves as the low-memory observer for the protocol
    proto->SetLowTransportMemoryObserver(
    [shptr=seastar::make_lw_shared<>(weak_from_this())] (const String& protoname, size_t requiredBytes) {
        K2DEBUG("lowmem notification from proto="<< protoname <<", requiredBytes="<< requiredBytes);
        seastar::weak_ptr<RPCDispatcher>& weakP = *shptr.get(); // the weak_ptr inside the lw_shared_ptr
        if (weakP) {
            weakP->_lowMemObserver(protoname, requiredBytes);
        }
    });

    return seastar::make_ready_future<>();
}

void RPCDispatcher::RegisterMessageObserver(Verb verb, MessageObserver_t observer) {
    K2DEBUG("Registering message observer for verb: " << verb);
    if (observer == nullptr) {
        _observers.erase(verb);
        K2DEBUG("Removing message observer for verb: " << verb);
        return;
    }
    K2DEBUG("Registering message observer for verb: " << verb);
    // we don't allow replacing verb observers. Raise an exception if there is an observer already
    auto emplace_pair = _observers.try_emplace(verb, observer);
    if (!emplace_pair.second) {
        throw DuplicateRegistrationException();
    }
}

void RPCDispatcher::Start() {
    K2DEBUG("start");
}

seastar::future<> RPCDispatcher::stop() {
    K2DEBUG("stop");

    // reset the messsage observer for each protocol as we're about to go away
    for(auto&& proto: _protocols) {
        proto.second->SetMessageObserver(nullptr);
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
void RPCDispatcher::_handleNewMessage(Request& request) {
    K2DEBUG("handling request for verb="<< request.verb <<", from ep="<< request.endpoint.GetURL());
    // see if this is a response
    if (request.metadata.IsResponseIDSet()) {
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
        K2DEBUG("Dispatching request for verb="<< request.verb <<", from ep="<< request.endpoint.GetURL());
        iter->second(request);
    }
    else {
        K2DEBUG("no observer for verb " << request.verb << ", from " << request.endpoint.GetURL());
    }
}

void RPCDispatcher::_send(Verb verb, std::unique_ptr<Payload> payload, Endpoint& endpoint, MessageMetadata meta) {
    K2DEBUG("Sending message for verb: " << verb << ", to endpoint=" << endpoint.GetURL());

    auto protoi = _protocols.find(endpoint.GetProtocol());
    if (protoi == _protocols.end()) {
        K2WARN("Unsupported protocol: "<< endpoint.GetProtocol());
        return;
    }
    protoi->second->Send(verb, std::move(payload), endpoint, std::move(meta));
}

void RPCDispatcher::Send(Verb verb, std::unique_ptr<Payload> payload, Endpoint& endpoint) {
    K2DEBUG("Plain send");
    MessageMetadata metadata;
    metadata.SetPayloadSize(payload->Size());
    _send(verb, std::move(payload), endpoint, std::move(metadata));
}

void RPCDispatcher::SendReply(std::unique_ptr<Payload> payload, Request& forRequest) {
    K2DEBUG("Reply send for request: " << forRequest.metadata.requestID);
    MessageMetadata metadata;
    metadata.SetResponseID(forRequest.metadata.requestID);
    metadata.SetPayloadSize(payload->Size());

    _send(ZEROVERB, std::move(payload), forRequest.endpoint, std::move(metadata));
}

seastar::future<std::unique_ptr<Payload>>
RPCDispatcher::SendRequest(Verb verb, std::unique_ptr<Payload> payload, Endpoint& endpoint, Duration timeout) {
    uint64_t msgid = _msgSequenceID++;
    K2DEBUG("Request send with msgid=" << msgid);

    // the promise gets fulfilled when prom for this msgid comes back.
    PayloadPromise prom;
    // record the promise so that we can fulfil it if we get a response
    MessageMetadata metadata;
    metadata.SetRequestID(msgid);
    metadata.SetPayloadSize(payload->Size());

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

void RPCDispatcher::RegisterLowTransportMemoryObserver(LowTransportMemoryObserver_t observer) {
    K2DEBUG("register low mem observer");
    if (observer == nullptr) {
        K2DEBUG("Setting default low transport memory observer");
        _lowMemObserver = [](const String& ttype, size_t suggestedBytes) {
            K2WARN("no low-mem observer installed. Transport: "<< ttype << ", requires release of "<< suggestedBytes << "bytes");
        };
    }
    else {
        _lowMemObserver = observer;
    }
}

std::unique_ptr<Endpoint> RPCDispatcher::GetEndpoint(String url) {
    K2DEBUG("Get endpoint for " << url)
    // temporary endpoint just so that we can see what the protocol is supposed to be
    auto ep = Endpoint::FromURL(url, nullptr);

    auto protoi = _protocols.find(ep->GetProtocol());
    if (protoi == _protocols.end()) {
        K2WARN("Unsupported protocol: "<< ep->GetProtocol());
        return nullptr;
    }
    return protoi->second->GetEndpoint(std::move(url));
}

}// namespace k2tx
