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

#include <optional>

#include <k2/dto/DocumentTypes.h>
#include <k2/dto/ControlPlaneOracle.h>

namespace k2 {
namespace dto {

class SerializableDocument {
public:
    // The document must be serialized in order. Schema will be enforced
    template <typename FieldType>
    void serializeNext(FieldType field) {
        DocumentFieldType ft = TToDocumentFieldType<FieldType>();
        if (fieldCursor >= schema.fields.size() || ft != schema.fields[fieldCursor].type) {
            throw new std::runtime_error("Schema not followed in document serialization");
        }

        fieldData.write(field);
        ++fieldCursor;
    }

    // Skip serializing the next field, for optional fields or partial updates
    void skipNext();

    // Deserialization can be in any order, but the preferred method is in-order
    template <typename FieldType>
    FieldType deserializeField(const String& name) {
        for (uint32_t i = 0; i < schema.fields.size(); ++i) {
            if (schema.fields[i].name == name) {
                return deserializeField<FieldType>(i);
            }
        }

        throw new std::runtime_error("Schema not followed in document deserialization");
    }

    void seekDocument(uint32_t fieldIndex);

    template <typename FieldType>
    FieldType deserializeField(uint32_t fieldIndex) {
        if (excludedFields.size() > 0 && excludedFields[fieldIndex]) {
            throw new std::runtime_error("Tried to deserialize an excluded field");
        }

        DocumentFieldType ft = TToDocumentFieldType<FieldType>();
        if (fieldCursor >= schema.fields.size() || ft != schema.fields[fieldCursor].type) {
            throw new std::runtime_error("Schema not followed in document deserialization");
        }

        if (fieldIndex != fieldCursor) {
            seekDocument(fieldIndex);
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
    SerializableDocument(const String& collection, Schema s) : 
            collectionName(collection), schemaName(s.name), schemaVersion(s.version), schema(s) {}

    String collectionName;
    String schemaName;
    uint32_t schemaVersion;
    uint32_t fieldCursor = 0;
    // Bitmap of fields that are excluded because they are optional or this is for a partial update
    std::vector<bool> excludedFields;
    Payload fieldData;

    // These fields are used by the client to build a request but are not serialized on the wire
    Schema schema;
    std::vector<String> partitionKeys;
    std::vector<String> rangeKeys;

    // These functions construct the keys on-the-fly based on the stringified individual fields
    Key getPartitionKey();
    Key getRangeKey();

    K2_PAYLOAD_FIELDS(schemaName, schemaVersion, excludedFields, fieldData);
};

// Helper function for the following macros
template <typename T>
std::optional<T> deserializeOptionalValueFromDoc(SerializableDocument& document) {
    if (document.excludedFields.size() && document.excludedFields[document.fieldCursor]) {
        return std::optional<T>();
    }

    return std::optional<T>(document.deserializeField<T>(document.fieldCursor));
}

// Convience macro that does the switch statement on the document field type for the user
// "func" must be the name of a templatized function that can be instantiated for all 
// docoument field types. The first arg to "func" is an optional of the field type, and a 
// variable number (at least 1) of arguments passed from the user
#define DO_ON_NEXT_DOC_FIELD(document, func, ...) \
    do { \
        switch ((document).schema.fields[(document).fieldCursor].type) { \
           case DocumentFieldType::STRING: \
           { \
               std::optional<String> value = deserializeOptionalValueFromDoc<String>((document)); \
               func<String>(std::move(value), __VA_ARGS__); \
           } \
               break; \
           case DocumentFieldType::UINT32T: \
           { \
               std::optional<uint32_t> value = deserializeOptionalValueFromDoc<uint32_t>((document)); \
               func<uint32_t>(std::move(value), __VA_ARGS__); \
           } \
               break; \
           default: \
               throw new std::runtime_error("Unknown type"); \
        } \
    } while (0) \
   

#define FOR_EACH_DOC_FIELD(document, func, ...) \
    do { \
        (document).fieldCursor = 0; \
        (document).fieldData.seek(0); \
        while ((document).fieldCursor < (document).schema.fields.size()) { \
            DO_ON_NEXT_DOC_FIELD((document), func, __VA_ARGS__); \
        } \
    } while (0) \

} // ns dto
} // ns k2
