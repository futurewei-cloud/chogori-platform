#pragma once
// This file contains DTOs for K2 AssignmentManager

namespace k2 {
namespace dto {

// Request to create a collection
struct AssignmentCreateRequest {
    String name;
    K2_PAYLOAD_FIELDS(name);
};

// Response to AssignmentCreateRequest
struct AssignmentCreateResponse {
    K2_PAYLOAD_COPYABLE;
};

// Request to offload a collection
struct AssignmentOffloadRequest {
    String name;
    K2_PAYLOAD_FIELDS(name);
};

// Response to AssignmentOffloadRequest
struct AssignmentOffloadResponse {
    K2_PAYLOAD_COPYABLE;
};

}  // namespace dto
}  // namespace k2
