#pragma once
// This file contains DTOs for K2 PartitionManager

namespace k2 {
namespace dto {

// Request to create a collection
struct PartitionRequest {
    String name;
    K2_PAYLOAD_FIELDS(name);
};

// Response to PartitionRequest
struct PartitionResponse {
    K2_PAYLOAD_COPYABLE;
};

}  // namespace dto
}  // namespace k2
