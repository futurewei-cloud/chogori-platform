//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
// stl
#include <cstdint> // for int types
#include <iostream>
#include <string>

namespace k2tx {
// This file contains definitions for RPC types
// The type for verbs in the RPC system
typedef uint8_t Verb;

// properly print verbs
inline std::ostream& operator<<(std::ostream & os, Verb& verb) {
    os << std::to_string(verb);
    return os;
}

// whenever we need a zero-value for verbs
const static Verb ZEROVERB = 0;

} // k2tx
