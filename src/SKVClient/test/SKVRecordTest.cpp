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

#define CATCH_CONFIG_MAIN

#include <skv/dto/SKVRecord.h>

#include "catch.hpp"

using namespace skv::http;

template <typename T>
void Test1Visitor(std::optional<T> value, const String& fieldName, int tmp) {
    (void) value;
    (void) fieldName;
    (void) tmp;
    throw std::runtime_error("Encountered unexpected type in test visitor");
}

template <>
void Test1Visitor<String>(std::optional<String> value, const String& fieldName, int tmp) {
    (void) tmp;
    REQUIRE(value);
    if (fieldName == "LastName") {
        REQUIRE(*value == "Baggins");
    } else {
        REQUIRE(*value == "Bilbo");
    }
}

template <>
void Test1Visitor<int32_t>(std::optional<int32_t> value, const String& fieldName, int tmp) {
    (void) tmp;
    REQUIRE(value);
    REQUIRE(fieldName == "Balance");
    REQUIRE(*value == 777);
}

// Simple partition and range keys, happy path tests
TEST_CASE("Test1: Basic SKVRecord tests") {
    dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::STRING, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    dto::SKVRecordBuilder builder("collection", std::make_shared<dto::Schema>(schema));

    builder.serializeNext<String>("Baggins");
    builder.serializeNext<String>("Bilbo");
    builder.serializeNext<int32_t>(777);

    auto doc = builder.build();

    std::vector<uint8_t> expectedKey{
        1, // Type byte
        66, // 'B'
        97, // 'a'
        103, // 'g'
        103, // 'g'
        105, // 'i'
        110, // 'n'
        115, // 's'
        0, // Terminator
        1, // Terminator
    };
    String partitionKey = doc.getPartitionKey();

    REQUIRE(expectedKey.size() == partitionKey.size());
    for (size_t i = 0; i < partitionKey.size(); ++i) {
        REQUIRE(expectedKey[i] == (uint8_t)partitionKey[i]);
    }

    std::vector<uint8_t> expectedRangeKey{
        1, // Type byte
        66, // 'B'
        105, // 'i'
        108, // 'l'
        98, // 'b'
        111, // 'o'
        0, // Terminator
        1, // Terminator
    };
    String rangeKey = doc.getRangeKey();

    REQUIRE(expectedRangeKey.size() == rangeKey.size());
    for (size_t i = 0; i < rangeKey.size(); ++i) {
        REQUIRE(expectedRangeKey[i] == (uint8_t)rangeKey[i]);
    }

    // Note that the following is not a normal use case of SKVRecord.
    // Normally a user will not serialize and deserialize out of the same SKVRecord, they
    // will serialize for a read or write request, and then deserialize a different SKVRecord
    // for a read result. Here we are manually rewinding the record so that we can test
    // deserialization
    doc.seekField(0);

    std::optional<String> lastName = doc.deserializeNext<String>();
    REQUIRE(lastName);
    REQUIRE(*lastName == "Baggins");
    std::optional<String> firstName = doc.deserializeNext<String>();
    REQUIRE(firstName);
    REQUIRE(*firstName == "Bilbo");
    std::optional<int32_t> balance = doc.deserializeNext<int32_t>();
    REQUIRE(balance);
    REQUIRE(*balance == 777);
    REQUIRE(true);

    doc.seekField(0);
    FOR_EACH_RECORD_FIELD(doc, Test1Visitor, 0); // Macro requires at least 1 extra arg, we just use "0"
}

TEST_CASE("Test2: invalid serialization tests") {
    dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::STRING, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    dto::SKVRecordBuilder builder("collection", std::make_shared<dto::Schema>(schema));

    try {
        // Invalid serialization order, should throw exception
        builder.serializeNext<int32_t>(777);
        REQUIRE(false);
    } catch (...) {}

    builder.serializeNext<String>("Baggins");
    builder.serializeNext<String>("Bilbo");
    builder.serializeNext<int32_t>(777);

    try {
        // Out-of-bounds serialzation
        builder.serializeNext<int32_t>(111);
        REQUIRE(false);
    } catch (...) {}
}

TEST_CASE("Test3: clone to other schema") {
    dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::STRING, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false}
    };
    schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    dto::Schema compatible_schema;
    compatible_schema.name = "compatible_schema";
    compatible_schema.version = 1;
    compatible_schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::STRING, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false}
    };
    compatible_schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    compatible_schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    dto::Schema noncompatible_schema;
    noncompatible_schema.name = "noncompatible_schema";
    noncompatible_schema.version = 1;
    noncompatible_schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::INT32T, "LastName", false, false},
            {dto::FieldType::INT32T, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false}
    };
    noncompatible_schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    noncompatible_schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    dto::SKVRecordBuilder builder("collection", std::make_shared<dto::Schema>(schema));
    builder.serializeNext<String>("a");
    builder.serializeNext<String>("b");
    builder.serializeNext<int32_t>(12);

    auto doc = builder.build();

    // Should clone without error
    dto::SKVRecord clone = doc.cloneToOtherSchema("collection",
                                    std::make_shared<dto::Schema>(compatible_schema));

    try {
        // Non-compatible schema, should throw exception
        dto::SKVRecord clone = doc.cloneToOtherSchema("collection",
                                    std::make_shared<dto::Schema>(noncompatible_schema));
        REQUIRE(false);
    } catch (...) {}
}

TEST_CASE("Test4: getSKVKeyRecord test") {
    dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::STRING, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false}
    };
    schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    dto::SKVRecordBuilder builder("collection", std::make_shared<dto::Schema>(schema));
    builder.serializeNull();
    builder.serializeNext<String>("b");
    builder.serializeNext<int32_t>(12);

    // Testing a typical use-case of getSKVKeyRecord where the storage is serialized
    // to payload and then read back later
    dto::SKVRecord key_record = builder.build();
    const dto::SKVRecord::Storage& storage = key_record.getStorage();
    MPackWriter w;
    w.write(storage);
    Binary buf;
    REQUIRE(w.flush(buf));
    MPackReader reader(buf);
    dto::SKVRecord::Storage read_storage{};
    REQUIRE(reader.read(read_storage));
    dto::SKVRecord reconstructed("collection", std::make_shared<dto::Schema>(schema),
                                     std::move(read_storage), true);

    std::optional<String> last = reconstructed.deserializeNext<String>();
    REQUIRE(!last.has_value());
    std::optional<String> first = reconstructed.deserializeNext<String>();
    REQUIRE(first.has_value());
    REQUIRE(first == "b");
    std::optional<int32_t> balance = reconstructed.deserializeNext<int32_t>();
    REQUIRE(!balance.has_value());
}

