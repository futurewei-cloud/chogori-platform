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

namespace skv::http::dto {

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
        visitNextField([](const auto&, auto&&){});
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
        keyStringsConstructed(false) {
    reader = MPackReader(this->storage.fieldData);
}

void SKVRecord::constructKeyStrings() {
    partitionKeys.resize(schema->partitionKeyFields.size());
    rangeKeys.resize(schema->rangeKeyFields.size());

    uint32_t beginCursor = getFieldCursor();
    seekField(0);
    auto numKeys = partitionKeys.size() + rangeKeys.size();
    for (size_t i = 0; i < partitionKeys.size(); ++i) {
        visitNextField([this, i](const auto& field, auto&& value) {
            if (value) {
                partitionKeys[i] = FieldToKeyString(*value);
            }
            else {
                partitionKeys[i] = field.nullLast ? NullLastToKeyString() : NullFirstToKeyString();
            }
        });
    }
    for (size_t i = partitionKeys.size(); i < numKeys; ++i) {
        visitNextField([this, i](const auto& field, auto&& value) {
            if (value) {
                rangeKeys[i] = FieldToKeyString(*value);
            }
            else {
                rangeKeys[i] = field.nullLast ? NullLastToKeyString() : NullFirstToKeyString();
            }
        });
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
void _fieldApplier(SchemaField, SKVRecordBuilder& builder, SKVRecord& rec) {
    auto opt = rec.deserializeNext<T>();
    if (opt) {
        builder.serializeNext(*opt);
    }
    else {
        builder.serializeNull();
    }
 }


// This method takes the SKVRecord extracts the key fields and creates a new SKVRecord with those fields
SKVRecord SKVRecord::getSKVKeyRecord() {
    if (!keyValuesAvailable) {
        throw KeyNotAvailableError("Cannot create an SKVKeyRecord from record without key values");
    }

    auto currentIndex = getFieldCursor();
    seekField(0);

    SKVRecordBuilder builder(collectionName, schema);

    size_t numKeyFields = schema->partitionKeyFields.size() + schema->rangeKeyFields.size();
    for (size_t i = 0; i < schema->fields.size(); ++i) {
        if (i < numKeyFields) {
            K2_DTO_CAST_APPLY_FIELD_VALUE(_fieldApplier, schema->fields[i], builder, *this);
         } else {
             builder.serializeNull();
         }
    }

    seekField(currentIndex);

    return builder.build();
}

// deepCopies an SKVRecord including copying (not sharing) the storage payload
SKVRecord SKVRecord::deepCopy() {
    Storage new_storage = storage.copy();
    return SKVRecord(collectionName, schema, std::move(new_storage), keyValuesAvailable);
}

} // ns dto
