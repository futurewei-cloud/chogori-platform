#pragma once
#include <k2/transport/RPCTypes.h>

enum MsgVerbs : k2::Verb {
    GETTSOSERVERINFO    = 100,
    GETTIMESTAMPBATCH   = 101
};
