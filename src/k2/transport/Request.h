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
#include <memory>

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
