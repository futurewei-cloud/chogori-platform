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

#include <optional>

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/SerializableDocument.h>

namespace k2 {
namespace dto {

void SerializableDocument::skipNext() {
    if (fieldCursor >= schema.fields.size()) {
        throw new std::runtime_error("Schema not followed in document serialization");
    }

    for (size_t i = 0; i < schema.partitionKeyFields.size(); ++i) {
        if (schema.partitionKeyFields[i] == fieldCursor) {
            partitionKeys[i] = schema.fields[i].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
        }

        if (schema.rangeKeyFields[i] == fieldCursor) {
            rangeKeys[i] = schema.fields[i].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
        }
    }

    if (excludedFields.size() == 0) {
        excludedFields = std::vector<bool>(schema.fields.size(), false);
    }

    excludedFields[fieldCursor] = true;
    ++fieldCursor;
}

// NoOp function to be used with the convience macros to get document fields
template <typename T>
void NoOp(std::optional<T> value, const String& fieldName, int n) {
    (void) value;
    (void) fieldName;
    (void) n;
};

void SerializableDocument::seekField(uint32_t fieldIndex) {
    if (fieldIndex == fieldCursor) {
        return;
    }

    if (fieldIndex >= schema.fields.size()) {
        throw new std::runtime_error("Tried to seek outside bounds");
    }

    if (fieldIndex < fieldCursor) {
        fieldCursor = 0;
        fieldData.seek(0);
    }

    while(fieldIndex != fieldCursor) {
        if (excludedFields.size() > 0 && excludedFields[fieldCursor]) {
            ++fieldCursor;
            continue;
        }

        DO_ON_NEXT_DOC_FIELD((*this), NoOp, 0);
    }
}

// We expose a shared payload in case the user wants to write it to file or otherwise 
// store it on their own. For normal K23SI operations the user does not need to touch this
Payload SerializableDocument::getSharedPayload() {
    return fieldData.share();
}

SerializableDocument::SerializableDocument(const String& collection, Schema s) : 
            collectionName(collection), schemaName(s.name), schemaVersion(s.version), schema(s) {
    fieldData = Payload([] () { return Binary(DEFAULT_SEGMENT_SIZE); });
    partitionKeys.resize(schema.partitionKeyFields.size());
    rangeKeys.resize(schema.partitionKeyFields.size());
}

String SerializableDocument::getPartitionKey() {
    String partitionKey("");
    for (const String& key : partitionKeys) {
        if (key == "") {
            throw new std::runtime_error("partition key field not set");
        }
        partitionKey += key;
    }

    return partitionKey;
}

String SerializableDocument::getRangeKey() {
    String rangeKey("");
    for (const String& key : rangeKeys) {
        if (key == "") {
            throw new std::runtime_error("partition key field not set");
        }
        rangeKey += key;
    }

    return rangeKey;
}

} // ns dto
} // ns k2
