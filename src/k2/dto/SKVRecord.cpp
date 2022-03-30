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
#include <k2/dto/Collection.h>
#include <k2/dto/shared/SKVRecord.h>
#include <k2/dto/FieldEncoding.h>

#include "Log.h"

namespace k2 {
namespace dto {

String SKVRecord::getPrintableString() const {
    return fmt::format(
        "{{collectionName={}, excludedFields={}, key={}, partitionKeys={}, rangeKeys={}}}",
        collectionName,
        storage.excludedFields,
        schema ? const_cast<SKVRecord*>(this)->getKey() : dto::Key{},
        partitionKeys,
        rangeKeys);
}

void SKVRecord::serializeNull() {
    if (fieldCursor >= schema->fields.size()) {
        throw NoFieldFoundException();
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

template <typename T>
void SKVRecord::serializeNext(T field) {
    if(isNan<T>(field)){
        throw NaNError("NaN type in serialization");
    }

    FieldType ft = TToFieldType<T>();
    if (fieldCursor >= schema->fields.size() || ft != schema->fields[fieldCursor].type) {
        throw TypeMismatchException("Schema not followed in record serialization");
    }

    for (size_t i = 0; i < schema->partitionKeyFields.size(); ++i) {
        if (schema->partitionKeyFields[i] == fieldCursor) {
            partitionKeys[i] = FieldToKeyString<T>(field);
        }
    }

    for (size_t i = 0; i < schema->rangeKeyFields.size(); ++i) {
        if (schema->rangeKeyFields[i] == fieldCursor) {
            rangeKeys[i] = FieldToKeyString<T>(field);
        }
    }

    storage.fieldData.write(field);
    ++fieldCursor;
}

// Explicit instantiation for all of the FieldType types, which allows us to have the template
// definiation in the .cpp file
template void SKVRecord::serializeNext<String>(String);
template void SKVRecord::serializeNext<int16_t>(int16_t);
template void SKVRecord::serializeNext<int32_t>(int32_t);
template void SKVRecord::serializeNext<int64_t>(int64_t);
template void SKVRecord::serializeNext<float>(float);
template void SKVRecord::serializeNext<double>(double);
template void SKVRecord::serializeNext<bool>(bool);
template void SKVRecord::serializeNext<std::decimal::decimal64>(std::decimal::decimal64);
template void SKVRecord::serializeNext<std::decimal::decimal128>(std::decimal::decimal128);

template <typename T>
std::optional<T> SKVRecord::deserializeField(uint32_t fieldIndex) {
    FieldType ft = TToFieldType<T>();
    std::optional<T> null_val = std::nullopt;

    if (fieldIndex >= schema->fields.size() || ft != schema->fields[fieldIndex].type) {

        throw TypeMismatchException(fmt::format("schema not followed in record deserialization for index {}", fieldIndex));
    }

    if (fieldIndex != fieldCursor) {
        seekField(fieldIndex);
    }

    ++fieldCursor;

    if (storage.excludedFields.size() > 0 && storage.excludedFields[fieldIndex]) {
        return null_val;
    }

    T value;
    bool success = storage.fieldData.read(value);
    if (!success) {
        throw DeserializationError("Deserialization of payload in SKVRecord failed");
    }

    return value;
}

// Explicit instantiation for all of the FieldType types, which allows us to have the template
// definiation in the .cpp file
template std::optional<String> SKVRecord::deserializeField<String>(uint32_t);
template std::optional<int16_t> SKVRecord::deserializeField<int16_t>(uint32_t);
template std::optional<int32_t> SKVRecord::deserializeField<int32_t>(uint32_t);
template std::optional<int64_t> SKVRecord::deserializeField<int64_t>(uint32_t);
template std::optional<float> SKVRecord::deserializeField<float>(uint32_t);
template std::optional<double> SKVRecord::deserializeField<double>(uint32_t);
template std::optional<bool> SKVRecord::deserializeField<bool>(uint32_t);
template std::optional<std::decimal::decimal64> SKVRecord::deserializeField<std::decimal::decimal64>(uint32_t);
template std::optional<std::decimal::decimal128> SKVRecord::deserializeField<std::decimal::decimal128>(uint32_t);


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

// We expose the storage in case the user wants to write it to file or otherwise
// store it on their own. For normal K23SI operations the user does not need to touch this.
// The user can later use the Storage-based constructor of the SKVRecord recreate a usable record.
const SKVStorage& SKVRecord::getStorage() {
    return storage;
}

// The constructor for an SKVRecord that a user of the SKV client would use to create a request
SKVRecord::SKVRecord(const String& collection, std::shared_ptr<Schema> s) :
            schema(s), collectionName(collection), keyValuesAvailable(true), keyStringsConstructed(true) {
    storage.schemaVersion = schema->version;
    storage.fieldData = Payload(Payload::DefaultAllocator());
    partitionKeys.resize(schema->partitionKeyFields.size());
    rangeKeys.resize(schema->rangeKeyFields.size());
}

// The constructor for an SKVRecord that is created by the SKV client to be returned to the user in a response
SKVRecord::SKVRecord(const String& collection, std::shared_ptr<Schema> s, SKVStorage&& storage, bool keyAvail) :
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

    String partitionKey(String::initialized_later(), keySize);
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

    String rangeKey(String::initialized_later(), keySize);
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
void skipFieldDataHelper(SchemaField fieldInfo, Payload& fieldData) {
    (void) fieldInfo;
    // TODO change this to payload skip when available
    T tmp{};
    fieldData.read(tmp);
}

// This method takes the SKVRecord extracts the key fields and creates a new SKVRecord with those fields
SKVRecord SKVRecord::getSKVKeyRecord() {
    if (!keyValuesAvailable) {
        throw KeyNotAvailableError("Cannot create an SKVKeyRecord from record without key values");
    }

    size_t num_keys = schema->partitionKeyFields.size() + schema->rangeKeyFields.size();
    SKVStorage key_storage = storage.share();

    // If size() == 0 then all fields were included, so resize the key_storage excluded fields
    if (key_storage.excludedFields.size() == 0) {
        key_storage.excludedFields.resize(schema->fields.size(), false);
    }

    // Exclude all value fields
    for (size_t i = num_keys; i < key_storage.excludedFields.size(); ++i) {
        key_storage.excludedFields[i] = true;
    }

    // Skipping key fields in fieldData so we can truncate the remaining value data in the payload
    for (size_t i = 0; i < num_keys; ++i) {
        if (!key_storage.excludedFields[i]) {
            K2_DTO_CAST_APPLY_FIELD_VALUE(skipFieldDataHelper, schema->fields[i], key_storage.fieldData);
        }
    }
    key_storage.fieldData.truncateToCurrent();
    key_storage.fieldData.seek(0);

    return SKVRecord(collectionName, schema, std::move(key_storage), true);
}

// deepCopies an SKVRecord including copying (not sharing) the storage payload
SKVRecord SKVRecord::deepCopy() {
    SKVStorage new_storage = storage.copy();
    return SKVRecord(collectionName, schema, std::move(new_storage), keyValuesAvailable);
}

} // ns dto
} // ns k2
