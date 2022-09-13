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

#include <skvhttp/dto/Log.h>
#include <skvhttp/dto/Collection.h>
#include <skvhttp/dto/FieldTypes.h>
#include <skvhttp/dto/ControlPlaneOracle.h>
#include <skvhttp/mpack/MPackSerialization.h>

namespace skv::http {

class K2TxnHandle;
class txn_testing;
class K23SITest;

namespace dto {
class SKVRecordBuilder;

// Thrown when a field is not found in the schema
struct NoFieldFoundException : public std::exception {
    String what_str;
    NoFieldFoundException(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

// Thrown when we were not able to deserialize a value correctly
struct DeserializationError : public std::exception {
    String what_str;
    DeserializationError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct NaNError : public std::exception {
    String what_str;
    NaNError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

// Thrown when getKey is called but the record does not have the values to construct it
// e.g. a returned record without the key fields projected
struct KeyNotAvailableError : public std::exception {
    String what_str;
    KeyNotAvailableError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

class SKVRecord {
public:
    // Deserialization can be in any order, but the preferred method is in-order
    template <typename T>
    std::optional<T> deserializeField(const String& name) {
        for (size_t i = 0; i < schema->fields.size(); ++i) {
            if (schema->fields[i].name == name) {
                return deserializeField<T>(i);
            }
        }

        throw NoFieldFoundException(fmt::format("schema not followed in record deserialization for name {}", name));
    }

    void seekField(uint32_t fieldIndex);

    template <typename T>
    std::optional<T> deserializeField(uint32_t fieldIndex) {
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

        T value{};
        bool success = reader.read(value);
        if (!success) {
            throw DeserializationError("Deserialization of payload in SKVRecord failed");
        }

        return value;
    }

    template <typename T>
    std::optional<T> deserializeNext() {
        return deserializeField<T>(fieldCursor);
    }

    uint32_t getFieldCursor() const;

    // These functions construct the keys on-the-fly based on the stringified individual fields
    String getPartitionKey();
    String getRangeKey();
    dto::Key getKey();

    // These are fields actually stored by the Chogori storage node, and returned
    // by a read request
    struct Storage {
        // Bitmap of fields that are excluded because they are optional or this is for a partial update
        std::vector<bool> excludedFields;
        // This is different from excludedFields above and needed because some use cases of the SKVRecord
        // (e.g. specifying a prefix scan for queries) will not completely serialize the record
        uint32_t serializedCursor = 0;
        Binary fieldData;
        uint32_t schemaVersion = 0;

        Storage share();
        Storage copy();
        K2_SERIALIZABLE_FMT(Storage, excludedFields, serializedCursor, fieldData, schemaVersion);
    };

    // We expose the storage in case the user wants to write it to file or otherwise
    // store it on their own. For normal K23SI operations the user does not need to touch this.
    // The user can later use the Storage-based constructor of the SKVRecord recreate a usable record.
    const Storage& getStorage();

    SKVRecord() = default;

    // The constructor for an SKVRecord that is created by the SKV client to be returned to the
    // user in a response
    SKVRecord(const String& collection, std::shared_ptr<Schema> s, Storage&& storage, bool keyValuesAvailable);

    SKVRecord cloneToOtherSchema(const String& collection, std::shared_ptr<Schema> other_schema);
    // deepCopies an SKVRecord including copying (not sharing) the storage payload
    SKVRecord deepCopy();

    // This method takes the SKVRecord extracts the key fields and creates a new SKVRecord with those fields
    SKVRecord getSKVKeyRecord();

    friend std::ostream& operator<<(std::ostream& os, const SKVRecord& rec) {
        return os << fmt::format(
            "{{collectionName={}, excludedFields={}, key={}, partitionKeys={}, rangeKeys={}}}",
            rec.collectionName,
            rec.storage.excludedFields,
            rec.schema ? const_cast<SKVRecord&>(rec).getKey() : dto::Key{},
            rec.partitionKeys,
            rec.rangeKeys);
    }

    void constructKeyStrings();

public:
    // These fields are used by the client to build a request but are not serialized on the wire
    std::shared_ptr<Schema> schema;
    String collectionName;
    Storage storage;
    std::vector<String> partitionKeys;
    std::vector<String> rangeKeys;
    uint32_t fieldCursor = 0;
    bool keyValuesAvailable = false; // Whether the values for key fields are in the storage payload
    bool keyStringsConstructed = false; // Whether the encoded keys are already in the vectors above
    MPackReader reader;

    friend class SKVRecordBuilder;
private:
    SKVRecord(const String& collection, std::shared_ptr<Schema> s);
public:
    template <typename Func>
    auto visitNextField(Func&& visitor) {
        const auto& field = schema->fields[fieldCursor];
        K2LOG_D(log::dto, "visiting field {}", field.name);
        return applyTyped(field, [this] (const auto& afr, auto&& visitor) {
            // the call to deserializeNext will take care of incrementing the current field cursor
            visitor(afr.field, deserializeNext<applied_type_t<decltype(afr)>>());
        }, std::forward<Func>(visitor));
    }

    template <typename Func>
    void visitRemainingFields(Func&& visitor) {
        K2LOG_D(log::dto, "visiting remaining fields at cursor {}", fieldCursor);
        while (fieldCursor < schema->fields.size()) {
            visitNextField(std::forward<Func>(visitor));
        }
    }
};

class SKVRecordBuilder {
public:
    template <typename T>
    void serializeNext(const T& field) {
        if (isNan(field)) {
            throw NaNError("NaN type in serialization");
        }

        using Tnr = typename std::remove_reference<T>::type;
        FieldType ft = TToFieldType<Tnr>();
        if (_record.fieldCursor >= _record.schema->fields.size() || ft != _record.schema->fields[_record.fieldCursor].type) {
            throw TypeMismatchException("Schema not followed in record serialization");
        }

        for (size_t i = 0; i < _record.schema->partitionKeyFields.size(); ++i) {
            if (_record.schema->partitionKeyFields[i] == _record.fieldCursor) {
                _record.partitionKeys[i] = FieldToKeyString(field);
            }
        }

        for (size_t i = 0; i < _record.schema->rangeKeyFields.size(); ++i) {
            if (_record.schema->rangeKeyFields[i] == _record.fieldCursor) {
                _record.rangeKeys[i] = FieldToKeyString(field);
            }
        }

        _writer.write(field);
        _record.storage.serializedCursor++;
        ++_record.fieldCursor;
    }

    std::shared_ptr<Schema> getSchema();

    const String& getCollectionName();

    // Serializing a Null value on the next field, for optional fields or partial updates
    void serializeNull();

    // The constructor for an SKVRecord that a user of the SKV client would use to create a request
    SKVRecordBuilder(const String& collection, std::shared_ptr<Schema> s): _record(collection, s){}

    // This method returns the built record
    SKVRecord build() {
        auto flushResult = _writer.flush(_record.storage.fieldData);
        K2ASSERT(log::dto, flushResult, "error during record packing");
        _record.reader = MPackReader(_record.storage.fieldData);
        _record.seekField(0);
        return std::move(_record);
    }

private:
    SKVRecord _record;
    MPackWriter _writer;
};

} // ns dto
} // ns k2
