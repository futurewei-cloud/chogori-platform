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

namespace k2 {
namespace dto {

class SKVRecord {
public:
    // The record must be serialized in order. Schema will be enforced
    template <typename T>
    void serializeNext(T field) {
        FieldType ft = TToFieldType<T>();
        if (fieldCursor >= schema->fields.size() || ft != schema->fields[fieldCursor].type) {
            throw new std::runtime_error("Schema not followed in record serialization");
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

    // Skip serializing the next field, for optional fields or partial updates
    void skipNext();

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
                skipNext();
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

        return std::nullopt;
    }

    void seekField(uint32_t fieldIndex);

    template <typename T>
    std::optional<T> deserializeField(uint32_t fieldIndex) {
        FieldType ft = TToFieldType<T>();
        std::optional<T> null_val = std::nullopt;

        if (fieldIndex >= schema->fields.size() || ft != schema->fields[fieldIndex].type) {
            throw new std::runtime_error("Schema not followed in record deserialization");
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
            throw new std::runtime_error("Deserialization of payload in SKVRecord failed");
        }

        return value;
    }

    template <typename T>
    std::optional<T> deserializeNext() {
        return deserializeField<T>(fieldCursor);
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

    // We expose a shared payload in case the user wants to write it to file or otherwise
    // store it on their own. For normal K23SI operations the user does not need to touch this
    Payload getSharedPayload();

    SKVRecord() = default;
    SKVRecord(const String& collection, std::shared_ptr<Schema> s);

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
    } storage;

    // These fields are used by the client to build a request but are not serialized on the wire
    std::shared_ptr<Schema> schema;
    String collectionName;
    std::vector<String> partitionKeys;
    std::vector<String> rangeKeys;
    uint32_t fieldCursor = 0;

    // These functions construct the keys on-the-fly based on the stringified individual fields
    String getPartitionKey() const;
    String getRangeKey() const;
    dto::Key getKey() const;
};

// Convience macro that does the switch statement on the record field type for the user
// "func" must be the name of a templatized function that can be instantiated for all
//  field types. The first arg to "func" is an optional of the field type,
// the second is a string for the name of the field
// and a variable number (at least 1) of arguments passed from the user
#define DO_ON_NEXT_RECORD_FIELD(record, func, ...) \
    do { \
        switch ((record).schema->fields[(record).fieldCursor].type) { \
           case k2::dto::FieldType::STRING: \
           { \
               std::optional<k2::String> value = (record).deserializeNext<k2::String>(); \
               func<k2::String>(std::move(value), (record).schema->fields[(record).fieldCursor-1].name, __VA_ARGS__); \
           } \
               break; \
           case k2::dto::FieldType::INT16T: \
           { \
               std::optional<int16_t> value = (record).deserializeNext<int16_t>(); \
               func<int16_t>(std::move(value), (record).schema->fields[(record).fieldCursor-1].name, __VA_ARGS__); \
           } \
               break; \
           case k2::dto::FieldType::INT32T: \
           { \
               std::optional<int32_t> value = (record).deserializeNext<int32_t>(); \
               func<int32_t>(std::move(value), (record).schema->fields[(record).fieldCursor-1].name, __VA_ARGS__); \
           } \
               break; \
           case k2::dto::FieldType::INT64T: \
           { \
               std::optional<int64_t> value = (record).deserializeNext<int64_t>(); \
               func<int64_t>(std::move(value), (record).schema->fields[(record).fieldCursor-1].name, __VA_ARGS__); \
           } \
               break; \
           case k2::dto::FieldType::FLOAT: \
           { \
               std::optional<float> value = (record).deserializeNext<float>(); \
               func<float>(std::move(value), (record).schema->fields[(record).fieldCursor-1].name, __VA_ARGS__); \
           } \
               break; \
           default: \
               throw new std::runtime_error("Unknown type"); \
        } \
    } while (0) \


#define FOR_EACH_RECORD_FIELD(record, func, ...) \
    do { \
        (record).seekField(0); \
        while ((record).fieldCursor < (record).schema->fields.size()) { \
            DO_ON_NEXT_RECORD_FIELD((record), func, __VA_ARGS__); \
        } \
    } while (0) \


// This macro is used to implement the templated read and write operations that
// automatically convert an SKVRecrod to a user-defined type. Fields must be declared
// in order of the schema and each primitive field must be wrapped in std::optional. It
// borrows the PayloadSerializableTrait machinery for nested support.
#define SKV_RECORD_FIELDS(...)                     \
    struct __K2PayloadSerializableTraitTag__ {};   \
    void __writeFields(k2::dto::SKVRecord& __record__) const {   \
        __record__.writeMany(__VA_ARGS__);            \
    }                                              \
    void __readFields(k2::dto::SKVRecord& __record__) {          \
        return __record__.readMany(__VA_ARGS__);      \
    }

} // ns dto
} // ns k2
