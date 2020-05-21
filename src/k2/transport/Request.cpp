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

#include "Request.h"
#include <k2/common/Log.h>

namespace k2 {

Request::Request(Verb verb, TXEndpoint& endpoint, MessageMetadata metadata, std::unique_ptr<Payload> payload):
    verb(verb),
    endpoint(endpoint),
    metadata(std::move(metadata)),
    payload(std::move(payload)) {
    K2DEBUG("ctor Request @" << ((void*)this)<< ", with verb=" << int(verb) << ", from " << endpoint.getURL());
}

Request::~Request() {
    K2DEBUG("dtor Request @" << ((void*)this));
}

Request::Request(Request&& o):
    verb(o.verb),
    endpoint(std::move(o.endpoint)),
    metadata(std::move(o.metadata)),
    payload(std::move(o.payload)) {
    o.verb = InternalVerbs::NIL;
    K2DEBUG("move Request @" << ((void*)this)<< ", with verb=" << int(verb) << ", from " << endpoint.getURL());
}

} // k2tx
