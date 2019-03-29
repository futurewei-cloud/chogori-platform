//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include "BaseTypes.h"

namespace k2tx {
class Channel{
public:
    Channel() = default;
    const String& Endpoint() const;
private:
    String _endpoint;
}; // class Channel
} // k2tx
