//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once

#include <k2/common/Common.h>
#include "RPCHeader.h"
#include "RPCTypes.h"
#include "TXEndpoint.h"

namespace k2 {

// This class is used to deliver a request to a message handler. It contains the message payload and some metadata
class Request{
public: // lifecycle
    // construct a request with the given verb, endpoint, metadata and payload
    Request(Verb verb, TXEndpoint& endpoint, MessageMetadata metadata, std::unique_ptr<Payload> payload);

    // move constructor
    Request(Request&& o);

    // destructor
    ~Request();

public: // fields

    // the verb for the request
    Verb verb;

    // the endpoint which sent the request
    TXEndpoint endpoint;

    // some message metadata
    MessageMetadata metadata;

    // the payload of this request
    std::unique_ptr<Payload> payload;

private: // don't need
    Request() = delete;
    Request(const Request& o) = delete;
    Request& operator=(const Request& o) = delete;
    Request& operator=(Request&& o) = delete;
};
} // k2
