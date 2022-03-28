/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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
#include <k2/dto/shared/FieldTypes.h>

#ifdef K2_PLATFORM_COMPILE
#include <k2/common/Log.h>
#endif

namespace k2::dto {

struct SchemaField {
    FieldType type;
    String name;
    // Ascending or descending sort order. Currently only relevant for
    // key fields, but could be used for secondary index in the future
    bool descending = false;
    // NULL first or last in sort order. Relevant for key fields and
    // for open-ended filter predicates
    bool nullLast = false;

    K2_PAYLOAD_FIELDS(type, name, descending, nullLast);
    K2_DEF_FMT(SchemaField, type, name, descending, nullLast);
};

struct Schema {
    String name;
    uint32_t version = 0;
    std::vector<SchemaField> fields;

    // All key fields must come before all value fields (by index), so that a key can be
    // constructed for a read request without knowing the schema version
    std::vector<uint32_t> partitionKeyFields;
    std::vector<uint32_t> rangeKeyFields;
    void setKeyFieldsByName(const std::vector<String>& keys, std::vector<uint32_t>& keyFields);
    void setPartitionKeyFieldsByName(const std::vector<String>& keys);
    void setRangeKeyFieldsByName(const std::vector<String>& keys);

    // Checks if the schema itself is well-formed (e.g. fields and fieldNames sizes match)
    // and returns a 400 status if not
    Status basicValidation() const;
    // Used to make sure that the partition and range key definitions do not change between versions
    Status canUpgradeTo(const dto::Schema& other) const;

    K2_PAYLOAD_FIELDS(name, version, fields, partitionKeyFields, rangeKeyFields);

    K2_DEF_FMT(Schema, name, version, fields, partitionKeyFields, rangeKeyFields);
};

}  // namespace k2::dto
