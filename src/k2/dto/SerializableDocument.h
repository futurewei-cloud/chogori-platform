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
    // The document must be serialized in order. Schema will be enforced
    template <typename FieldType>
    void serializeNext(FieldType field, const Schema& schema) {
        DocumentFieldType ft = TToDocumentFieldType<FieldType>();
        if (fieldCursor >= schema.fields.size() || ft != schema.fields[fieldCursor]) {
            throw new std::runtime_error("Schema not followed in document serialization");
        }

        fieldData.write(field);
        ++fieldCursor;
    }

    // Skip serializing the next field, for optional fields or partial updates
    void skipNext(const Schema& schema);

    // Deserialization can be in any order, but the preferred method is in-order
    template <typename FieldType>
    FieldType deserializeField(const String& name, const Schema& schema) {
        for (uint32_t i = 0; i < schema.fields.size(); ++i) {
            if (schema.fieldNames[i] == name) {
                return deserializeField<FieldType>(i, schema);
            }
        }

        throw new std::runtime_error("Schema not followed in document deserialization");
    }

    void seekDocument(uint32_t fieldIndex, const Schema& schema);

    template <typename FieldType>
    FieldType deserializeField(uint32_t fieldIndex, const Schema& schema) {
        if (excludedFields.size() > 0 && excludedFields[fieldIndex]) {
            throw new std::runtime_error("Tried to deserialize an excluded field");
        }

        DocumentFieldType ft = TToDocumentFieldType<FieldType>();
        if (fieldCursor >= schema.fields.size() || ft != schema.fields[fieldCursor]) {
            throw new std::runtime_error("Schema not followed in document deserialization");
        }

        if (fieldIndex != fieldCursor) {
            seekDocument(fieldIndex, schema);
        }

        FieldType value;
        fieldData.read(value);
        ++fieldCursor;
        return value;
    }

    // We expose a shared payload in case the user wants to write it to file or otherwise 
    // store it on their own. For normal K23SI operations the user does not need to touch this
    Payload getSharedPayload();

    SerializableDocument() = default;
    SerializableDocument(const SerializableDocument& doc, bool copyPayload) : schemaID(doc.schemaID),
        schemaVersion(doc.schemaVersion), fieldCursor(doc.fieldCursor), excludedFields(doc.excludedFields) {
        if (copyPayload) {
            fieldData = doc.fieldData.copy();
            fieldData.seek(0);
        } else {
            fieldData = doc.fieldData.share();
            fieldData.seek(0);
        }
    }

    // Bitmap of fields that are excluded because they are optional or this is for a partial update
    uint64_t schemaID;
    uint32_t schemaVersion;
    uint32_t fieldCursor = 0;
    std::vector<bool> excludedFields;
    Payload fieldData;

    // These functions construct the keys on-the-fly based on fieldData
    Key getPartitionKey();
    Key getRangeKey();

    K2_PAYLOAD_FIELDS(schemaID, schemaVersion, excludedFields, fieldData);
};


#define DO_ON_NEXT_DOC_FIELD(document, schema, func, ...) \
    do { \
        switch ((schema).fields[(document).fieldCursor]) { \
           case DocumentFieldType::STRING: \
               func<String>((document).deserializeField<String>((document).fieldCursor), (schema), __VA_ARGS__); \
               break; \
           case DocumentFieldType::UINT32T: \
               func<uint32_t>((document).deserializeField<uint32_t>((document).fieldCursor), (schema), __VA_ARGS__); \
               break; \
           default: \
               throw new std::runtime_error("Unknown type"); \
        } \
    } while (0) \
   

// TODO excluded fields?
#define FOR_EACH_DOC_FIELD(document, schema, func, ...) \
    do { \
        (document).fieldCursor = 0; \
        (document).fieldData.seek(0); \
        while ((document).fieldCursor < (schema).fields.size()) { \
            DO_ON_NEXT_DOC_FIELD((document), (schema), func, __VA_ARGS__); \
        } \
    } while (0) \

} // ns dto
} // ns k2
