//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "Request.h"
#include "Log.h"

namespace k2tx {

Request::Request(Verb verb, Endpoint& endpoint, MessageMetadata metadata, std::unique_ptr<Payload> payload):
    verb(verb),
    endpoint(endpoint),
    metadata(std::move(metadata)),
    payload(std::move(payload)) {
    K2DEBUG("Request " << verb << ", from " << endpoint.GetURL());
}

Request::~Request() {
    K2DEBUG("dtor Request " << verb << ", from " << endpoint.GetURL());
}

Request::Request(Request&& o):
    verb(o.verb),
    endpoint(std::move(o.endpoint)),
    metadata(std::move(o.metadata)),
    payload(std::move(o.payload)) {
    o.verb = ZEROVERB;
    K2DEBUG("copy Request " << verb << ", from " << endpoint.GetURL());
}

} // k2tx
