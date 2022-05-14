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

#include "SKVRecord.h"

namespace k2 {
namespace dto {

void SKVRecordBuilder::serializeNull() {
    if (_record.fieldCursor >= _record.schema->fields.size()) {
        throw NoFieldFoundException();
    }

    for (size_t i = 0; i < _record.schema->partitionKeyFields.size(); ++i) {
        if (_record.schema->partitionKeyFields[i] == _record.fieldCursor) {
            _record.partitionKeys[i] = _record.schema->fields[_record.fieldCursor].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
        }
    }

    for (size_t i = 0; i < _record.schema->rangeKeyFields.size(); ++i) {
        if (_record.schema->rangeKeyFields[i] == _record.fieldCursor) {
            _record.rangeKeys[i] = _record.schema->fields[_record.fieldCursor].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
        }
    }

    if (_record.storage.excludedFields.size() == 0) {
        _record.storage.excludedFields = std::vector<bool>(_record.schema->fields.size(), false);
    }

    _record.storage.excludedFields[_record.fieldCursor] = true;
    ++_record.fieldCursor;
}

// NoOp function to be used with the convience macros to get document fields
template <typename T>
void NoOp(std::optional<T> value, const String& fieldName, int n) {
    (void) value;
    (void) fieldName;
    (void) n;
};

uint32_t SKVRecord::getFieldCursor() const {
    return fieldCursor;
}

void SKVRecord::seekField(uint32_t fieldIndex) {
    if (fieldIndex > schema->fields.size()) {
        throw NoFieldFoundException();
    }

    if (fieldIndex == fieldCursor) {
        return;
    }

    if (fieldIndex < fieldCursor) {
        fieldCursor = 0;
        reader = MPackReader(storage.fieldData);
    }

    while(fieldIndex != fieldCursor) {
        if (storage.excludedFields.size() > 0 && storage.excludedFields[fieldCursor]) {
            ++fieldCursor;
            continue;
        }

        DO_ON_NEXT_RECORD_FIELD((*this), NoOp, 0);
    }
}

// We expose the storage in case the user wants to write it to file or otherwise
// store it on their own. For normal K23SI operations the user does not need to touch this.
// The user can later use the Storage-based constructor of the SKVRecord recreate a usable record.
const SKVRecord::Storage& SKVRecord::getStorage() {
    return storage;
}

// The constructor for an SKVRecord that a user of the SKV client would use to create a request
SKVRecord::SKVRecord(const String& collection, std::shared_ptr<Schema> s) :
            schema(s), collectionName(collection), keyValuesAvailable(true), keyStringsConstructed(true) {
    storage.schemaVersion = schema->version;
    partitionKeys.resize(schema->partitionKeyFields.size());
    rangeKeys.resize(schema->rangeKeyFields.size());
}

// The constructor for an SKVRecord that is created by the SKV client to be returned to the user in a response
SKVRecord::SKVRecord(const String& collection, std::shared_ptr<Schema> s, Storage&& storage, bool keyAvail) :
        schema(s), collectionName(collection), storage(std::move(storage)), keyValuesAvailable(keyAvail),
        keyStringsConstructed(false) {}

template <typename T>
void SKVRecord::makeKeyString(std::optional<T> value, const String& fieldName, int tmp) {
    // tmp variable is need for vararg macro expansion
    (void) tmp;
    (void) fieldName;
    for (size_t i = 0; i < schema->partitionKeyFields.size(); ++i) {
        // Subtracting 1 from fieldCursor here and below because this function gets called
        // after the deserialize function moves the fieldCursor
        if (schema->partitionKeyFields[i] == fieldCursor-1) {
            if (value.has_value()) {
                partitionKeys[i] = FieldToKeyString<T>(*value);
            } else {
                partitionKeys[i] = schema->fields[fieldCursor-1].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
            }
        }
    }

    for (size_t i = 0; i < schema->rangeKeyFields.size(); ++i) {
         if (schema->rangeKeyFields[i] == fieldCursor-1) {
            if (value.has_value()) {
                rangeKeys[i] = FieldToKeyString<T>(*value);
            } else {
                rangeKeys[i] = schema->fields[fieldCursor-1].nullLast ? NullLastToKeyString() : NullFirstToKeyString();
            }
        }
    }
}

void SKVRecord::constructKeyStrings() {
    partitionKeys.resize(schema->partitionKeyFields.size());
    rangeKeys.resize(schema->rangeKeyFields.size());

    uint32_t beginCursor = getFieldCursor();
    seekField(0);
    for (size_t i = 0; i < partitionKeys.size() + rangeKeys.size() && i < schema->fields.size(); ++i) {
        // Need the extra 0 argument for vararg macro expansion
        DO_ON_NEXT_RECORD_FIELD(*this, makeKeyString, 0);
    }

    seekField(beginCursor);
    keyStringsConstructed = true;
}

String SKVRecord::getPartitionKey() {
    if (!keyValuesAvailable) {
        throw KeyNotAvailableError("Cannot get a partition key from this SKVRecord");
    }
    if (!keyStringsConstructed) {
        constructKeyStrings();
    }

    size_t keySize = 0;
    for (const String& key : partitionKeys) {
        keySize += key.size();
    }

    // This should not be possible in practice but it is here defensively
    if (schema->partitionKeyFields.size() == 0) {
        K2LOG_W(log::dto, "SKVRecord schema has no partitionKeyFields");
        return String("");
    }

    String partitionKey;
    partitionKey.resize(keySize);
    size_t position = 0;
    for (const String& key : partitionKeys) {
        std::copy(key.begin(), key.end(), partitionKey.begin() + position);
        position += key.size();
    }

    return partitionKey;
}

String SKVRecord::getRangeKey() {
    if (!keyValuesAvailable) {
        throw KeyNotAvailableError("Cannot get a range key from this SKVRecord");
    }
    if (!keyStringsConstructed) {
        constructKeyStrings();
    }

    size_t keySize = 0;
    for (const String& key : rangeKeys) {
        keySize += key.size();
    }

    if (schema->rangeKeyFields.size() == 0) {
        return String("");
    }

    String rangeKey;
    rangeKey.resize(keySize);
    size_t position = 0;
    for (const String& key : rangeKeys) {
        std::copy(key.begin(), key.end(), rangeKey.begin() + position);
        position += key.size();
    }

    return rangeKey;
}

dto::Key SKVRecord::getKey() {
    if (!schema) return dto::Key{};

    return dto::Key {
        .schemaName = schema->name,
        .partitionKey = getPartitionKey(),
        .rangeKey = getRangeKey()
    };
}

SKVRecord::Storage SKVRecord::Storage::share() {
    return SKVRecord::Storage {
        excludedFields,
        fieldData,
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

SKVRecord SKVRecord::cloneToOtherSchema(const String& collection, std::shared_ptr<Schema> other_schema) {
    // Check schema compatibility (same number of fields and same types in order)
    if (other_schema->fields.size() != schema->fields.size()) {
        throw TypeMismatchException("Schemas do not match for SKVRecord cloneToOtherSchema: size mismatch");
    }
    for (size_t i = 0; i < schema->fields.size(); ++i) {
        if (schema->fields[i].type != other_schema->fields[i].type) {
            throw TypeMismatchException(fmt::format("Schemas do not match for SKVRecord cloneToOtherSchema - type mismatch on field {}: have({}) vs other({})", schema->fields[i].name, schema->fields[i].type, other_schema->fields[i].type));
        }
    }

    return SKVRecord(collection, other_schema, storage.share(), keyValuesAvailable);
}

template <typename T>
void fieldApplier(SchemaField fieldInfo, SKVRecordBuilder& builder, MPackReader& reader) {
    (void) fieldInfo;
    T field{};
    if (!reader.read(field)) {
        throw KeyNotAvailableError("Unable to read key field from buffer");
    }
    builder.serializeNext(field);
}

// This method takes the SKVRecord extracts the key fields and creates a new SKVRecord with those fields
SKVRecord SKVRecord::getSKVKeyRecord() {
    if (!keyValuesAvailable) {
        throw KeyNotAvailableError("Cannot create an SKVKeyRecord from record without key values");
    }

    SKVRecordBuilder builder(collectionName, schema);
    MPackReader reader(storage.fieldData);

    for (size_t i = 0; i < schema->partitionKeyFields.size() + schema->rangeKeyFields.size(); ++i) {
        if (!storage.excludedFields[i]) {
            K2_DTO_CAST_APPLY_FIELD_VALUE(fieldApplier, schema->fields[i], builder, reader);
        }
        else {
            builder.serializeNull();
        }
    }

    return builder.build();
}

// deepCopies an SKVRecord including copying (not sharing) the storage payload
SKVRecord SKVRecord::deepCopy() {
    Storage new_storage = storage.copy();
    return SKVRecord(collectionName, schema, std::move(new_storage), keyValuesAvailable);
}

} // ns dto
} // ns k2
