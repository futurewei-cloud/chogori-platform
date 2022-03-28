/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include <k2/dto/shared/Status.h>
#include <k2/dto/shared/Schema.h>

#include "Collection.h"
#include "Timestamp.h"

#include "Log.h"
// This file contains DTOs for K2 ControlPlaneOracle

namespace k2::dto {

struct CPOClientException : public std::exception {
    String what_str;
    CPOClientException(String s) : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override{ return what_str.c_str();}
};


// Request to create a collection
struct CollectionCreateRequest {
    // The metadata which describes the collection K2 should create
    CollectionMetadata metadata;
    // Only relevant for range partitioned collections. Contains the key range
    // endpoints for each partition.
    std::vector<String> rangeEnds;

    K2_PAYLOAD_FIELDS(metadata, rangeEnds);
    K2_DEF_FMT(CollectionCreateRequest, metadata, rangeEnds);
};

// Response to CollectionCreateRequest
struct CollectionCreateResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(CollectionCreateResponse);
};

// Request to get a collection
struct CollectionGetRequest {
    // The name of the collection to get
    String name;
    K2_PAYLOAD_FIELDS(name);
    K2_DEF_FMT(CollectionGetRequest, name);
};

// Response to CollectionGetRequest
struct CollectionGetResponse {
    // The collection we found
    Collection collection;
    K2_PAYLOAD_FIELDS(collection);
    K2_DEF_FMT(CollectionGetResponse, collection);
};

struct CollectionDropRequest {
    String name;
    K2_PAYLOAD_FIELDS(name);
    K2_DEF_FMT(CollectionDropRequest, name);
};

struct CollectionDropResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(CollectionDropResponse);
};

// Request to create a schema and attach it to a collection
// If schemaName already exists, it creates a new version
struct CreateSchemaRequest {
    String collectionName;
    Schema schema;
    K2_PAYLOAD_FIELDS(collectionName, schema);
    K2_DEF_FMT(CreateSchemaRequest, collectionName, schema);
};

// Response to CreateSchemaRequest
struct CreateSchemaResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(CreateSchemaResponse);
};

// Get all versions of all schemas associated with a collection
struct GetSchemasRequest {
    String collectionName;
    K2_PAYLOAD_FIELDS(collectionName);
    K2_DEF_FMT(GetSchemasRequest, collectionName);
};

struct GetSchemasResponse {
    std::vector<Schema> schemas;
    K2_PAYLOAD_FIELDS(schemas);
    K2_DEF_FMT(GetSchemasResponse, schemas);
};

struct GetTSOEndpointsRequest {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(GetTSOEndpointsRequest);
};

struct GetTSOEndpointsResponse {
    std::vector<String> endpoints;
    K2_PAYLOAD_FIELDS(endpoints);
    K2_DEF_FMT(GetTSOEndpointsResponse, endpoints);
};

struct GetPersistenceEndpointsRequest {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(GetPersistenceEndpointsRequest);
};

struct GetPersistenceEndpointsResponse {
    std::vector<String> endpoints;
    K2_PAYLOAD_FIELDS(endpoints);
    K2_DEF_FMT(GetPersistenceEndpointsResponse, endpoints);
};

struct HeartbeatRequest {
    // The target uses this to determine if its responses are succeeding or if it needs to die.
    uint64_t lastToken;

    // Time between heartbeats and how many lost heartbeats are needed to consider a target dead.
    // This is needed as part of the request so that targets can monitor if they should soft shutdown and
    // so that this information can be controlled by the CPO rather than part of the target config.
    Duration interval;
    uint32_t deadThreshold;

    K2_PAYLOAD_FIELDS(lastToken, interval, deadThreshold);
    K2_DEF_FMT(HeartbeatRequest, lastToken, interval, deadThreshold);
};

struct HeartbeatResponse {
    // The instance ID, unique in the cluster at a given moment in time
    String ID;
    // Role specific metadata, such as collection assignment status for a nodepool.
    // This is only used for logging and debugging.
    String roleMetadata;
    // Available endpoints that this instance can be reached at
    std::vector<String> endpoints;
    // The target uses this to determine if its responses are succeeding or if it needs to die.
    uint64_t echoToken;
    K2_PAYLOAD_FIELDS(ID, roleMetadata, endpoints, echoToken);
    K2_DEF_FMT(HeartbeatResponse, ID, roleMetadata, endpoints, echoToken);
};

}  // namespace k2::dto
