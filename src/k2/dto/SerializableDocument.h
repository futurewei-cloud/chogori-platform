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

#include <k2/dto/DocumentTypes.h>
#include <k2/dto/ControlPlaneOracle.h>

namespace k2 {
namespace dto {

class SerializableDocument {
public:

    template <typename FieldType, DocumentFieldType type>
    void serializeField(FieldType field, const String& name);

    template <typename FieldType, DocumentFieldType type>
    void serializeField(FieldType field, uint32_t fieldIndex);

    template <typename FieldType, DocumentFieldType type>
    FieldType deserializeField(const String& name);

    template <typename FieldType, DocumentFieldType type>
    FieldType deserializeField(uint32_t fieldIndex);

    // We expose the payload in case the user wants to write it to file or otherwise 
    // store it on their own. For normal K23SI operations the user does not need to touch this variable
    Payload fieldData;

    SerializableDocument(String collectionName, const Schema& schema);
    SerializableDocument();

    String collectionName;
    Schema schema;

    Key getPartitionKey();
    Key getRangeKey();

    // The following members are the ones that are serialized, along with fieldData
    // Bitmap of fields that are excluded because they are optional or this is for a partial update
    std::vector<uint64_t> excludedFields;
    String schemaName;
    uint32_t schemaVersion;

    K2_PAYLOAD_FIELDS(fieldData, excludedFields, schemaName, schemaVersion);
};

} // ns dto
} // ns k2
