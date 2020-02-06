#include "Collection.h"

#pragma once
// This file contains DTOs for K2 AssignmentManager

namespace k2 {
namespace dto {

// Request to create a collection
struct AssignmentCreateRequest {
    String collectionName;
    Partition partition;
    K2_PAYLOAD_FIELDS(collectionName);
};

// Response to AssignmentCreateRequest
struct AssignmentCreateResponse {
    K2_PAYLOAD_COPYABLE;
};

// Request to offload a collection
struct AssignmentOffloadRequest {
    String collectionName;
    K2_PAYLOAD_FIELDS(collectionName);
};

// Response to AssignmentOffloadRequest
struct AssignmentOffloadResponse {
    K2_PAYLOAD_COPYABLE;
};

}  // namespace dto
}  // namespace k2
