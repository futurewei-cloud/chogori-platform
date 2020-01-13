#include "Collection.h"
// This file contains DTOs for K2 ControlPlaneOracle

namespace k2 {
namespace dto {

// Request to create a collection
struct CollectionCreateRequest {
    // The metadata which describes the collection K2 should create
    CollectionMetadata metadata;
    K2_PAYLOAD_FIELDS(metadata);
};

// Response to CollectionCreateRequest
struct CollectionCreateResponse {
    K2_PAYLOAD_COPYABLE;
};

// Request to get a collection
struct CollectionGetRequest {
    // The name of the collection to get
    String name;
    K2_PAYLOAD_FIELDS(name);
};

// Response to CollectionGetRequest
struct CollectionGetResponse {
    // The collection we found
    Collection collection;
    K2_PAYLOAD_FIELDS(collection);
};

}  // namespace dto
}  // namespace k2
