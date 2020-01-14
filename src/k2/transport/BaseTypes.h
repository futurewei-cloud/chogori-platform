//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
#include <memory>
#include <functional>

#include <seastar/net/socket_defs.hh> // for socket_address
#include <seastar/core/future.hh>

#include "Payload.h"
#include <k2/common/Common.h>
#include "Request.h"
#include "Status.h"
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
