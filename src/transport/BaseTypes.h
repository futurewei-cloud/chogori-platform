//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
#include <seastar/net/socket_defs.hh> // for socket_address

#include "common/Payload.h"
#include "common/Common.h"

namespace k2 {
// This file contains definitions for the base types we may want to use in the transport codebase

// SocketAddress alias
typedef seastar::socket_address SocketAddress;
} // k2
