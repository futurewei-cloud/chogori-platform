//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
// stl
#include <cstdint> // for int types

namespace k2tx {
// This file contains definitions for RPC types
// The type for verbs in the RPC system
typedef uint8_t Verb;

// whenever we need a zero-value for verbs
const static Verb ZEROVERB = 0;

} // k2tx
