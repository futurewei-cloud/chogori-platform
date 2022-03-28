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
#include <functional>

#include <seastar/net/socket_defs.hh> // for socket_address
#include <seastar/core/future.hh>

#include <k2/common/Common.h>
#include <k2/dto/shared/Status.h>
#include "Payload.h"
#include "Request.h"
#include "TXEndpoint.h"

namespace k2 {
// This file contains definitions for the base types we may want to use in the transport codebase

// SocketAddress alias
typedef seastar::socket_address SocketAddress;

// The type for Message observers
// TODO See if we can use something faster than std::function.
// Benchmark indicates 20ns penalty per runtime call
// See https://www.boost.org/doc/libs/1_69_0/doc/html/function/faq.html
typedef std::function<void(Request&& request)> RequestObserver_t;

// The type for RPC request observers. This is meant to be used with the RPC* API of RPCDispatcher
template <class RequestType_t, class ResponseType_t>
using RPCRequestObserver_t = std::function<seastar::future<std::tuple<k2::Status, ResponseType_t>>(RequestType_t&& request)>;

// The type for observers of channel failures
typedef std::function<void(TXEndpoint& endpoint, std::exception_ptr exc)> FailureObserver_t;

// the type of a low memory observer. This function will be called when a transport requires a release of
// some memory
typedef std::function<void(const String& ttype, size_t requiredBytes)> LowTransportMemoryObserver_t;

// the type of a function we can call to notify that we're low on memory. Function is called with required
// number of bytes to release
typedef std::function<void(size_t requiredNumberOfBytes)> LowMemoryObserver_t;

} // namespace k2
