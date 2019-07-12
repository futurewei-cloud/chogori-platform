//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once
// stl
#include <cstdint> // for int types
#include <iostream>
#include <string>

namespace k2 {
// This file contains definitions for RPC types
// The type for verbs in the RPC system

//
//  Verb describes particular service within K2 endpoint
//
typedef uint8_t Verb;

//
//  Verbs that K2 is using internally
//
class KnownVerbs
{
public:
    enum Verbs : Verb
    {
        None = 0,               //  Currently is used with responses
        ZEROVERB = None,        //  Transport naming for None
        PartitionMessages = 1,  //  K2 Partition management service
        PartitionManager = 2
    };
};

} // namespace k2
