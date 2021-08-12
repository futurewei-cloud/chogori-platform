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

#include <k2/dto/Collection.h>
#include <k2/dto/FieldTypes.h>
#include <k2/dto/ControlPlaneOracle.h>
#include "Log.h"
namespace k2 {

class K2TxnHandle;
class txn_testing;
class K23SITest;

namespace dto {

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
    enum CursorType : uint8_t {
        SERIALIZER,
        DESERIALIZER
    };
    // The record must be serialized in order. Schema will be enforced
    template <typename T>
    void serializeNext(T field) {
        if(isNan<T>(field)){
            throw NaNError("NaN type in serialization");
        }

        FieldType ft = TToFieldType<T>();
        if (serializerFieldCursor >= schema->fields.size() || ft != schema->fields[serializerFieldCursor].type) {
            throw TypeMismatchException(fmt::format("Schema not followed in record serialization for index {}",
                                        serializerFieldCursor));
        }

        for (size_t i = 0; i < schema->partitionKeyFields.size(); ++i) {
            if (schema->partitionKeyFields[i] == serializerFieldCursor) {
                partitionKeys[i] = FieldToKeyString<T>(field);
            }
        }

        for (size_t i = 0; i < schema->rangeKeyFields.size(); ++i) {
            if (schema->rangeKeyFields[i] == serializerFieldCursor) {
                rangeKeys[i] = FieldToKeyString<T>(field);
            }
        }

        storage.fieldData.write(field);
        ++serializerFieldCursor;
    }

    // Serializing a Null value on the next field, for optional fields or partial updates
    void serializeNull();

    // Used as part of the SKV_RECORD_FIELDS macro for converting to user-defined types
    // All primitive fields must be wrapped in std::optional
    template <typename T, typename... ArgsT>
    void writeMany(T& value, ArgsT&... args) {
        // For nested type support
        if constexpr(isPayloadSerializableType<T>()) {
            value.__writeFields(*this);
            writeMany(args...);
        } else {
            if (!value) {
                serializeNull();
            } else {
                serializeNext<typename std::decay_t<decltype(value)>::value_type>(*value);
            }
            writeMany(args...);
        }
    }
    // no-arg version to satisfy the template expansion above in the terminal case
    void writeMany() {}

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
            throw TypeMismatchException(fmt::format("schema not followed in record deserialization for index {}",
                                        fieldIndex));
        }

        if (fieldIndex != deserializerFieldCursor || deserializerFieldCursor == 0) {
            seekField(fieldIndex);
        }

        ++deserializerFieldCursor;

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

    template <typename T>
    std::optional<T> deserializeNext() {
        return deserializeField<T>(deserializerFieldCursor);
    }

    // Used as part of the SKV_RECORD_FIELDS macro for converting to user-defined types
    // All primitive fields must be wrapped in std::optional
    template <typename T, typename... ArgsT>
    void readMany(T& value, ArgsT&... args) {
        // For nested type support
        if constexpr(isPayloadSerializableType<T>()) {
            value.__readFields(*this);
            readMany(args...);
        } else {
            value = deserializeNext<typename std::decay_t<decltype(value)>::value_type>();
            readMany(args...);
        }
    }
    // no-arg version to satisfy the template expansion above in the terminal case
    void readMany() {}

    uint32_t getFieldCursor(CursorType cursorType) const;

    // These fields are used by the client to build a request but are not serialized on the wire
    std::shared_ptr<Schema> schema;
    String collectionName;

    // These functions construct the keys on-the-fly based on the stringified individual fields
    String getPartitionKey();
    String getRangeKey();
    dto::Key getKey();

    // These are fields actually stored by the Chogori storage node, and returned
    // by a read request
    struct Storage {
        // Bitmap of fields that are excluded because they are optional or this is for a partial update
        std::vector<bool> excludedFields;
        Payload fieldData;
        uint32_t schemaVersion = 0;

        Storage share();
        Storage copy();
        K2_PAYLOAD_FIELDS(excludedFields, fieldData, schemaVersion);
        K2_DEF_FMT(Storage, excludedFields, schemaVersion);
    };

    // We expose the storage in case the user wants to write it to file or otherwise
    // store it on their own. For normal K23SI operations the user does not need to touch this.
    // The user can later use the Storage-based constructor of the SKVRecord recreate a usable record.
    const Storage& getStorage();

    SKVRecord() = default;
    // The constructor for an SKVRecord that a user of the SKV client would use to create a request
    SKVRecord(const String& collection, std::shared_ptr<Schema> s);
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

    void friend inline to_json(nlohmann::json& j, const SKVRecord& o) {
        j = nlohmann::json{{ "skv_record", fmt::format("{}", o) }};
    }

    void friend inline from_json(const nlohmann::json&, SKVRecord&) {
        throw std::runtime_error("cannot construct SKVRecord from json");
    }

private:
    void constructKeyStrings();
    template <typename T>
    void makeKeyString(std::optional<T> value, const String& fieldName, int tmp);
    Storage storage;

    std::vector<String> partitionKeys;
    std::vector<String> rangeKeys;
    uint32_t serializerFieldCursor = 0;    // field possition during serialization
    uint32_t deserializerFieldCursor = 0;  // field possition during deserialization 
    bool keyValuesAvailable = false; // Whether the values for key fields are in the storage payload
    bool keyStringsConstructed = false; // Whether the encoded keys are already in the vectors above

    friend class k2::K2TxnHandle;
    friend class k2::txn_testing;
    friend class k2::K23SITest;
};

// Convience macro that does the switch statement on the record field type for the user
// "func" must be the name of a templatized function that can be instantiated for all
//  field types. The first arg to "func" is an optional of the field type,
// the second is a string for the name of the field
// and a variable number (at least 1) of arguments passed from the user
#define DO_ON_NEXT_RECORD_FIELD(record, func, ...)                                                                    \
    do {                                                                                                              \
        auto& field = (record).schema->fields[(record).getFieldCursor(k2::dto::SKVRecord::CursorType::DESERIALIZER)];                                             \
        switch (field.type) {                                                                                         \
            case k2::dto::FieldType::STRING: {                                                                        \
                std::optional<k2::String> value = (record).deserializeNext<k2::String>();                             \
                func<k2::String>(std::move(value), field.name, __VA_ARGS__);                                          \
            } break;                                                                                                  \
            case k2::dto::FieldType::INT16T: {                                                                        \
                std::optional<int16_t> value = (record).deserializeNext<int16_t>();                                   \
                func<int16_t>(std::move(value), field.name, __VA_ARGS__);                                             \
            } break;                                                                                                  \
            case k2::dto::FieldType::INT32T: {                                                                        \
                std::optional<int32_t> value = (record).deserializeNext<int32_t>();                                   \
                func<int32_t>(std::move(value), field.name, __VA_ARGS__);                                             \
            } break;                                                                                                  \
            case k2::dto::FieldType::INT64T: {                                                                        \
                std::optional<int64_t> value = (record).deserializeNext<int64_t>();                                   \
                func<int64_t>(std::move(value), field.name, __VA_ARGS__);                                             \
            } break;                                                                                                  \
            case k2::dto::FieldType::FLOAT: {                                                                         \
                std::optional<float> value = (record).deserializeNext<float>();                                       \
                func<float>(std::move(value), field.name, __VA_ARGS__);                                               \
            } break;                                                                                                  \
            case k2::dto::FieldType::DOUBLE: {                                                                        \
                std::optional<double> value = (record).deserializeNext<double>();                                     \
                func<double>(std::move(value), field.name, __VA_ARGS__);                                              \
            } break;                                                                                                  \
            case k2::dto::FieldType::BOOL: {                                                                          \
                std::optional<bool> value = (record).deserializeNext<bool>();                                         \
                func<bool>(std::move(value), field.name, __VA_ARGS__);                                                \
            } break;                                                                                                  \
            case k2::dto::FieldType::DECIMAL64: {                                                                     \
                std::optional<std::decimal::decimal64> value = (record).deserializeNext<std::decimal::decimal64>();   \
                func<std::decimal::decimal64>(std::move(value), field.name, __VA_ARGS__);                             \
            } break;                                                                                                  \
            case k2::dto::FieldType::DECIMAL128: {                                                                    \
                std::optional<std::decimal::decimal128> value = (record).deserializeNext<std::decimal::decimal128>(); \
                func<std::decimal::decimal128>(std::move(value), field.name, __VA_ARGS__);                            \
            } break;                                                                                                  \
            case k2::dto::FieldType::FIELD_TYPE: {                                                                    \
                std::optional<k2::dto::FieldType> value = (record).deserializeNext<k2::dto::FieldType>();             \
                func<k2::dto::FieldType>(std::move(value), field.name, __VA_ARGS__);                                  \
            } break;                                                                                                  \
            default:                                                                                                  \
                auto msg = fmt::format("unknown type {} for field {}", field.type, field.name);                       \
                throw k2::dto::TypeMismatchException(msg);                                                            \
        }                                                                                                             \
    } while (0)

#define FOR_EACH_RECORD_FIELD(record, func, ...)                             \
    do {                                                                     \
        (record).seekField(0);                                               \
        while ((record).getFieldCursor(k2::dto::SKVRecord::CursorType::DESERIALIZER) < (record).schema->fields.size()) { \
            DO_ON_NEXT_RECORD_FIELD((record), func, __VA_ARGS__);            \
        }                                                                    \
    } while (0)

// This macro is used to implement the templated read and write operations that
// automatically convert an SKVRecrod to a user-defined type. Fields must be declared
// in order of the schema and each primitive field must be wrapped in std::optional. It
// borrows the PayloadSerializableTrait machinery for nested support.
#define SKV_RECORD_FIELDS(...)                                 \
    struct __K2PayloadSerializableTraitTag__ {};               \
    void __writeFields(k2::dto::SKVRecord& __record__) const { \
        __record__.writeMany(__VA_ARGS__);                     \
    }                                                          \
    void __readFields(k2::dto::SKVRecord& __record__) {        \
        return __record__.readMany(__VA_ARGS__);               \
    }

} // ns dto
} // ns k2
