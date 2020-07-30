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

struct SchemaField {
    DocumentFieldType type;
    String name;
    // Ascending or descending sort order. Currently only relevant for 
    // key fields, but could be used for secondary index in the future
    bool descending = false;
    // NULL first or last in sort order. Relevant for key fields and 
    // for open-ended filter predicates
    bool nullLast = false;
    K2_PAYLOAD_FIELDS(type, name, descending, nullLast);
};

struct Schema {
    String name;
    uint32_t version = 0;
    std::vector<SchemaField> fields;
    std::vector<uint32_t> partitionKeyFields;
    std::vector<uint32_t> rangeKeyFields;

    void setKeyFieldsByName(const std::vector<String>& keys, std::vector<uint32_t>& keyFields) {
        for (const String& keyName : keys) {
            bool found = false;
            for (size_t i = 0; i < fields.size(); ++i) {
                if (keyName == fields[i].name) {
                    found = true;
                    keyFields.push_back(i);
                    break;
                }
            }

            if (!found) {
                throw new std::runtime_error("Failed to find field by name");
            }
        }
    }

    void setPartitionKeyFieldsByName(const std::vector<String>& keys) {
        setKeyFieldsByName(keys, partitionKeyFields);
    }

    void setRangeKeyFieldsByName(const std::vector<String>& keys) {
        setKeyFieldsByName(keys, rangeKeyFields);
    }

    // Checks if the schema itself is well-formed (e.g. fields and fieldNames sizes match)
    // and returns a 400 status if not
    Status basicValidation() const {
        std::unordered_set<String> uniqueNames;
        for (const dto::SchemaField& field : fields) {
            auto [it, isUnique] = uniqueNames.insert(field.name);
            if (!isUnique) {
                return Statuses::S400_Bad_Request("Duplicated field name in schema");
            }
        }

        if (partitionKeyFields.size() == 0) {
            K2WARN("Bad CreateSchemaRequest: No partitionKeyFields defined");
            return Statuses::S400_Bad_Request("No partitionKeyFields defined");
        }

        std::unordered_set<uint32_t> uniqueIndex;
        for (uint32_t keyIndex : partitionKeyFields) {
            auto [it, isUnique] = uniqueIndex.insert(keyIndex);
            if (!isUnique) {
                return Statuses::S400_Bad_Request("Duplicated field in partitionKeys");
            }

            if (keyIndex >= fields.size()) {
                K2WARN("Bad CreateSchemaRequest: partitionKeyField index out of bounds");
                return Statuses::S400_Bad_Request("partitionKeyField index out of bounds");
            }
        }

        uniqueIndex.clear();
        for (uint32_t keyIndex : rangeKeyFields) {
            auto [it, isUnique] = uniqueIndex.insert(keyIndex);
            if (!isUnique) {
                return Statuses::S400_Bad_Request("Duplicated field in partitionKeys");
            }

            if (keyIndex >= fields.size()) {
                K2WARN("Bad CreateSchemaRequest: rangeKeyField index out of bounds");
                return Statuses::S400_Bad_Request("rangeKeyField index out of bounds");
            }
        }

        return Statuses::S200_OK("basic validation passed");
    }

    // Used to make sure that the partition and range key definitions do not change between versions
    Status canUpgradeTo(const dto::Schema& other) const {
        if (partitionKeyFields.size() != other.partitionKeyFields.size()) {
            return Statuses::S409_Conflict("partitionKey fields of schema versions do not match");
        }

        if (rangeKeyFields.size() != other.rangeKeyFields.size()) {
            return Statuses::S409_Conflict("rangeKey fields of schema versions do not match");
        }

        for (size_t i = 0; i < partitionKeyFields.size(); ++i) {
            uint32_t a_fieldIndex = partitionKeyFields[i];
            const String& a_name = fields[a_fieldIndex].name;
            dto::DocumentFieldType a_type = fields[a_fieldIndex].type;

            uint32_t b_fieldIndex = other.partitionKeyFields[i];
            const String& b_name = other.fields[b_fieldIndex].name;
            dto::DocumentFieldType b_type = other.fields[b_fieldIndex].type;

            if (b_name != a_name || b_type != a_type) {
                return Statuses::S409_Conflict("partitionKey fields of schema versions do not match");
            }
        }

        for (size_t i = 0; i < rangeKeyFields.size(); ++i) {
            uint32_t a_fieldIndex = rangeKeyFields[i];
            const String& a_name = fields[a_fieldIndex].name;
            dto::DocumentFieldType a_type = fields[a_fieldIndex].type;

            uint32_t b_fieldIndex = other.rangeKeyFields[i];
            const String& b_name = other.fields[b_fieldIndex].name;
            dto::DocumentFieldType b_type = other.fields[b_fieldIndex].type;

            if (b_name != a_name || b_type != a_type) {
                return Statuses::S409_Conflict("rangeKey fields of schema versions do not match");
            }
        }

        return Statuses::S200_OK("Upgrade compatible");
    }

    K2_PAYLOAD_FIELDS(name, version, fields, partitionKeyFields, rangeKeyFields);
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
    K2_PAYLOAD_EMPTY;
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
