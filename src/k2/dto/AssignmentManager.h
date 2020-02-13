#include "Collection.h"

#pragma once
// This file contains DTOs for K2 AssignmentManager

namespace k2 {
namespace dto {

// Request to create a collection
struct AssignmentCreateRequest {
    CollectionMetadata collectionMeta;
    Partition partition;
    K2_PAYLOAD_FIELDS(collectionMeta, partition);
};

// Response to AssignmentCreateRequest
struct AssignmentCreateResponse {
    Partition assignedPartition;
    K2_PAYLOAD_FIELDS(assignedPartition);
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
