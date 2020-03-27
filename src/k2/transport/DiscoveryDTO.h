#pragma once
#include <k2/common/Common.h>
#include "PayloadSerialization.h"

namespace k2 {

// Request for listing the supported endpoints by a node
struct ListEndpointsRequest {
    K2_PAYLOAD_COPYABLE;
};

// Response, specifying the supported endpoints by a node
struct ListEndpointsResponse {
    std::vector<String> endpoints;
    K2_PAYLOAD_FIELDS(endpoints);
};

} // ns k2
