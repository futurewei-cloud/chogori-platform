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

#include <algorithm>
#include <optional>

#include <k2/appbase/AppEssentials.h>
#include <k2/dto/SKVRecord.h>

namespace k2 {
namespace dto {

void SKVRecord::skipNext() {
    if (fieldCursor >= schema->fields.size()) {
        throw std::runtime_error("Schema not followed in record serialization");
    }

    for (size_t i = 0; i < schema->partitionKeyFields.size(); ++i) {
        if (schema->partitionKeyFields[i] == fieldCursor) {
            partitionKeys[i] = schema->fields[fieldCursor].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
        }
    }

    for (size_t i = 0; i < schema->rangeKeyFields.size(); ++i) {
        if (schema->rangeKeyFields[i] == fieldCursor) {
            rangeKeys[i] = schema->fields[fieldCursor].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
        }
    }

    if (storage.excludedFields.size() == 0) {
        storage.excludedFields = std::vector<bool>(schema->fields.size(), false);
    }

    storage.excludedFields[fieldCursor] = true;
    ++fieldCursor;
}

// NoOp function to be used with the convience macros to get document fields
template <typename T>
void NoOp(std::optional<T> value, const String& fieldName, int n) {
    (void) value;
    (void) fieldName;
    (void) n;
};

void SKVRecord::seekField(uint32_t fieldIndex) {
    if (fieldIndex >= schema->fields.size()) {
        throw std::runtime_error("Tried to seek outside bounds");
    }

    if (fieldIndex == fieldCursor) {
        return;
    }

    if (fieldIndex < fieldCursor) {
        fieldCursor = 0;
        storage.fieldData.seek(0);
    }

    while(fieldIndex != fieldCursor) {
        if (storage.excludedFields.size() > 0 && storage.excludedFields[fieldCursor]) {
            ++fieldCursor;
            continue;
        }

        DO_ON_NEXT_RECORD_FIELD((*this), NoOp, 0);
    }
}

// We expose a shared payload in case the user wants to write it to file or otherwise 
// store it on their own. For normal K23SI operations the user does not need to touch this
Payload SKVRecord::getSharedPayload() {
    return storage.fieldData.shareAll();
}

SKVRecord::SKVRecord(const String& collection, std::shared_ptr<Schema> s) : 
            schema(s), collectionName(collection) {
    storage.schemaVersion = schema->version; 
    storage.fieldData = Payload(Payload::DefaultAllocator);
    partitionKeys.resize(schema->partitionKeyFields.size());
    rangeKeys.resize(schema->rangeKeyFields.size());
}

String SKVRecord::getPartitionKey() const {
    size_t keySize = 0;
    for (const String& key : partitionKeys) {
        keySize += key.size();
    }

    // This should not be possible in practice but it is here defensively
    if (schema->partitionKeyFields.size() == 0) {
        K2WARN("SKVRecord schema has no partitionKeyFields");
        return String("");
    }

    String partitionKey(String::initialized_later(), keySize);
    size_t position = 0;
    for (const String& key : partitionKeys) {
        std::copy(key.begin(), key.end(), partitionKey.begin() + position);
        position += key.size();
    }

    return partitionKey;
}

String SKVRecord::getRangeKey() const {
    size_t keySize = 0;
    for (const String& key : rangeKeys) {
        keySize += key.size();
    }

    if (schema->rangeKeyFields.size() == 0) {
        return String("");
    }

    String rangeKey(String::initialized_later(), keySize);
    size_t position = 0;
    for (const String& key : rangeKeys) {
        std::copy(key.begin(), key.end(), rangeKey.begin() + position);
        position += key.size();
    }

    return rangeKey;
}

dto::Key SKVRecord::getKey() const {
    return dto::Key {
        .schemaName = schema->name,
        .partitionKey = getPartitionKey(),
        .rangeKey = getRangeKey()
    };
}

SKVRecord::Storage SKVRecord::Storage::share() {
    return SKVRecord::Storage {
        excludedFields,
        fieldData.shareAll(),
        schemaVersion
    };
}

SKVRecord::Storage SKVRecord::Storage::copy() {
    return SKVRecord::Storage {
        excludedFields,
        fieldData.copy(),
        schemaVersion
    };
}

} // ns dto
} // ns k2
