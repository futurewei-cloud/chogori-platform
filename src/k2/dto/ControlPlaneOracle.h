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

#include "Collection.h"
#include "DocumentTypes.h"

// This file contains DTOs for K2 ControlPlaneOracle

namespace k2 {
namespace dto {

// Request to create a collection
struct CollectionCreateRequest {
    // The metadata which describes the collection K2 should create
    CollectionMetadata metadata;
    // the endpoints of the k2 cluster to use for setting up this collection
    std::vector<String> clusterEndpoints;
    // Only relevant for range partitioned collections. Contains the key range
    // endpoints for each partition.
    std::vector<String> rangeEnds;

    K2_PAYLOAD_FIELDS(metadata, clusterEndpoints, rangeEnds);
};

// Response to CollectionCreateRequest
struct CollectionCreateResponse {
    K2_PAYLOAD_EMPTY;
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

struct KeyFieldDef {
    uint32_t index; // Field by index to use in key
    bool descending; // Ascending or descending sort order
    bool nullLast; // NULL first or last in sort order
    K2_PAYLOAD_COPYABLE;
};

struct Schema {
    String name;
    uint64_t id;
    uint32_t version;
    std::vector<DocumentFieldType> fields;
    std::vector<String> fieldNames;
    std::vector<KeyFieldDef> partitionKeyFields;
    std::vector<KeyFieldDef> rangeKeyFields;
    K2_PAYLOAD_FIELDS(name, version, fields, fieldNames, partitionKeyFields, rangeKeyFields);
};

// Request to create a schema and attach it to a collection
// If schemaName already exists, it creates a new version
struct CreateSchemaRequest {
    String collectionName;
    Schema schema;
    K2_PAYLOAD_FIELDS(collectionName, schema);
};

// Response to CreateSchemaRequest
struct CreateSchemaResponse {
    uint64_t id; // ID of the created schema, client has the rest already
    K2_PAYLOAD_COPYABLE;
};

// Get all versions of all schemas associated with a collection
struct GetSchemasRequest {
    String collectionName;
    K2_PAYLOAD_FIELDS(collectionName);
};

struct GetSchemasResponse {
    std::vector<Schema> schemas;
    K2_PAYLOAD_FIELDS(schemas);
};

}  // namespace dto
}  // namespace k2
