//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "Request.h"
#include "common/Log.h"

namespace k2 {

Request::Request(Verb verb, TXEndpoint& endpoint, MessageMetadata metadata, std::unique_ptr<Payload> payload):
    verb(verb),
    endpoint(endpoint),
    metadata(std::move(metadata)),
    payload(std::move(payload)) {
    K2DEBUG("ctor Request @" << ((void*)this)<< ", " << verb << ", from " << endpoint.getURL());
}

Request::~Request() {
    K2DEBUG("dtor Request @" << ((void*)this));
}

Request::Request(Request&& o):
    verb(o.verb),
    endpoint(std::move(o.endpoint)),
    metadata(std::move(o.metadata)),
    payload(std::move(o.payload)) {
    o.verb = KnownVerbs::ZEROVERB;
    K2DEBUG("move Request @" << ((void*)this)<< ", " << verb << ", from " << endpoint.getURL());
}

} // k2tx
