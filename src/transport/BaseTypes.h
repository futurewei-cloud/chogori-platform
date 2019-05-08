//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
#include <seastar/core/sstring.hh>
#include "common/Payload.h"
#include "common/Common.h"

namespace k2tx {
// This file contains definitions for the base types we may want to use in the transport codebase

// the string type
typedef k2::String String;

// duration used in a few places to specify timeouts and such
typedef std::chrono::steady_clock::duration Duration;

// The Payload we use
typedef k2::Payload Payload;

} // k2tx
