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

#include "ControlPlaneOracle.h"
#include "Log.h"

namespace skv::http::dto {

void Schema::setKeyFieldsByName(const std::vector<String>& keys, std::vector<uint32_t>& keyFields) {
    for (const String& keyName : keys) {
        bool found = false;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (keyName == fields[i].name) {
                found = true;
                keyFields.push_back(i);
                break;
            }
        }
        K2ASSERT(log::dto, found, "failed to find field by name");
    }
}

void Schema::setPartitionKeyFieldsByName(const std::vector<String>& keys) {
    setKeyFieldsByName(keys, partitionKeyFields);
}

void Schema::setRangeKeyFieldsByName(const std::vector<String>& keys) {
    setKeyFieldsByName(keys, rangeKeyFields);
}

// Checks if the schema itself is well-formed (e.g. field names are unique)
// and returns a 400 status if not
Status Schema::basicValidation() const {
    std::unordered_set<String> uniqueNames;
    for (const dto::SchemaField& field : fields) {
        auto [it, isUnique] = uniqueNames.insert(field.name);
        if (!isUnique) {
            return Statuses::S400_Bad_Request("Duplicated field name in schema");
        }
    }

    if (partitionKeyFields.size() == 0) {
        K2LOG_W(log::dto, "Bad CreateSchemaRequest: No partitionKeyFields defined");
        return Statuses::S400_Bad_Request("No partitionKeyFields defined");
    }

    std::unordered_set<uint32_t> uniqueIndex;
    std::vector<bool> foundIndexes(partitionKeyFields.size() + rangeKeyFields.size(), false);
    uint32_t maxIndex = 0;
    for (uint32_t keyIndex : partitionKeyFields) {
        auto [it, isUnique] = uniqueIndex.insert(keyIndex);
        if (!isUnique) {
            return Statuses::S400_Bad_Request("Duplicated field in partitionKeys");
        }

        if (keyIndex >= fields.size()) {
            K2LOG_W(log::dto, "Bad CreateSchemaRequest: partitionKeyField index out of bounds");
            return Statuses::S400_Bad_Request("partitionKeyField index out of bounds");
        }

        if (keyIndex >= foundIndexes.size()) {
            K2LOG_W(log::dto, "Bad CreateSchemaRequest: All key fields must precede all value fields");
            return Statuses::S400_Bad_Request("All key fields must precede all value fields");
        }
        foundIndexes[keyIndex] = true;
        maxIndex = std::max(maxIndex, keyIndex);
    }

    uniqueIndex.clear();
    for (uint32_t keyIndex : rangeKeyFields) {
        auto [it, isUnique] = uniqueIndex.insert(keyIndex);
        if (!isUnique) {
            return Statuses::S400_Bad_Request("Duplicated field in partitionKeys");
        }

        if (keyIndex >= fields.size()) {
            K2LOG_W(log::dto, "Bad CreateSchemaRequest: rangeKeyField index out of bounds");
            return Statuses::S400_Bad_Request("rangeKeyField index out of bounds");
        }

        if (keyIndex >= foundIndexes.size()) {
            K2LOG_W(log::dto, "Bad CreateSchemaRequest: All key fields must precede all value fields");
            return Statuses::S400_Bad_Request("All key fields must precede all value fields");
        }
        foundIndexes[keyIndex] = true;
        maxIndex = std::max(maxIndex, keyIndex);
    }

    for (uint32_t i = 0; i < maxIndex; ++i) {
        if (!foundIndexes[i]) {
            K2LOG_W(log::dto, "Bad CreateSchemaRequest: All key fields must precede all value fields");
            return Statuses::S400_Bad_Request("All key fields must precede all value fields");
        }
    }

    return Statuses::S200_OK("basic validation passed");
}

// Used to make sure that the partition and range key definitions do not change between versions
Status Schema::canUpgradeTo(const dto::Schema& other) const {
    if (partitionKeyFields.size() != other.partitionKeyFields.size()) {
        return Statuses::S409_Conflict("partitionKey fields of schema versions do not match");
    }

    if (rangeKeyFields.size() != other.rangeKeyFields.size()) {
        return Statuses::S409_Conflict("rangeKey fields of schema versions do not match");
    }

    // Key field names, types, and positions must remain invariant among versions so that
    // a read request can be constructed without regards to a particular version
    for (size_t i = 0; i < partitionKeyFields.size(); ++i) {
        uint32_t a_fieldIndex = partitionKeyFields[i];
        const String& a_name = fields[a_fieldIndex].name;
        dto::FieldType a_type = fields[a_fieldIndex].type;

        uint32_t b_fieldIndex = other.partitionKeyFields[i];
        if (a_fieldIndex != b_fieldIndex) {
            return Statuses::S409_Conflict("partitionKey fields of schema versions do not match");
        }

        const String& b_name = other.fields[b_fieldIndex].name;
        dto::FieldType b_type = other.fields[b_fieldIndex].type;

        if (b_name != a_name || b_type != a_type) {
            return Statuses::S409_Conflict("partitionKey fields of schema versions do not match");
        }
    }

    for (size_t i = 0; i < rangeKeyFields.size(); ++i) {
        uint32_t a_fieldIndex = rangeKeyFields[i];
        const String& a_name = fields[a_fieldIndex].name;
        dto::FieldType a_type = fields[a_fieldIndex].type;

        uint32_t b_fieldIndex = other.rangeKeyFields[i];
        if (a_fieldIndex != b_fieldIndex) {
            return Statuses::S409_Conflict("partitionKey fields of schema versions do not match");
        }

        const String& b_name = other.fields[b_fieldIndex].name;
        dto::FieldType b_type = other.fields[b_fieldIndex].type;

        if (b_name != a_name || b_type != a_type) {
            return Statuses::S409_Conflict("rangeKey fields of schema versions do not match");
        }
    }

    return Statuses::S200_OK("Upgrade compatible");
}

}  // namespace skv::http::dto
