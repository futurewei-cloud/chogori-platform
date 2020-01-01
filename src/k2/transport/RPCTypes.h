//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
// stl
#include <cstdint> // for int types
#include <iostream>

namespace k2 {
// This file contains definitions for RPC types
// The type for verbs in the RPC system

//
//  Verb describes particular service within K2 endpoint
//
typedef uint8_t Verb;

// Verbs used by K2 internally
enum InternalVerbs : k2::Verb {
    NIL = 0 // used for messages where the verb doesn't matter
};

} // namespace k2
