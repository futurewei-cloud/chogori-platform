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

#include <k2/dto/SKVRecord.h>

#include "catch2/catch.hpp"

template <typename T>
void Test1Visitor(std::optional<T> value, const k2::String& fieldName, int tmp) {
    (void) value;
    (void) fieldName;
    (void) tmp;
    throw std::runtime_error("Encountered unexpcted type in test visitor");
}

template <>
void Test1Visitor<k2::String>(std::optional<k2::String> value, const k2::String& fieldName, int tmp) {
    (void) tmp;
    REQUIRE(value);
    if (fieldName == "LastName") {
        REQUIRE(*value == "Baggins");
    } else {
        REQUIRE(*value == "Bilbo");
    }
}

template <>
void Test1Visitor<int32_t>(std::optional<int32_t> value, const k2::String& fieldName, int tmp) {
    (void) tmp;
    REQUIRE(value);
    REQUIRE(fieldName == "Balance");
    REQUIRE(*value == 777);
}

// Simple partition and range keys, happy path tests
TEST_CASE("Test1: Basic SKVRecord tests") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::INT32T, "Balance", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<k2::String>{"FirstName"});

    k2::dto::SKVRecord doc("collection", std::make_shared<k2::dto::Schema>(schema));

    doc.serializeNext<k2::String>("Baggins");
    doc.serializeNext<k2::String>("Bilbo");
    doc.serializeNext<int32_t>(777);

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
    k2::String partitionKey = doc.getPartitionKey();

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
    k2::String rangeKey = doc.getRangeKey();

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

    std::optional<k2::String> lastName = doc.deserializeNext<k2::String>();
    REQUIRE(lastName);
    REQUIRE(*lastName == "Baggins");
    std::optional<k2::String> firstName = doc.deserializeNext<k2::String>();
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
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::INT32T, "Balance", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<k2::String>{"FirstName"});

    k2::dto::SKVRecord doc("collection", std::make_shared<k2::dto::Schema>(schema));

    try {
        // Invalid serialization order, should throw exception
        doc.serializeNext<int32_t>(777);
        REQUIRE(false);
    } catch (...) {}

    doc.serializeNext<k2::String>("Baggins");
    doc.serializeNext<k2::String>("Bilbo");
    doc.serializeNext<int32_t>(777);

    try {
        // Out-of-bounds serialzation
        doc.serializeNext<int32_t>(111);
        REQUIRE(false);
    } catch (...) {}
}

TEST_CASE("Test3: clone to other schema") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::INT32T, "Balance", false, false}
    };
    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<k2::String>{"FirstName"});

    k2::dto::Schema compatible_schema;
    compatible_schema.name = "compatible_schema";
    compatible_schema.version = 1;
    compatible_schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::INT32T, "Balance", false, false}
    };
    compatible_schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName"});
    compatible_schema.setRangeKeyFieldsByName(std::vector<k2::String>{"FirstName"});

    k2::dto::Schema noncompatible_schema;
    noncompatible_schema.name = "noncompatible_schema";
    noncompatible_schema.version = 1;
    noncompatible_schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::INT32T, "LastName", false, false},
            {k2::dto::FieldType::INT32T, "FirstName", false, false},
            {k2::dto::FieldType::INT32T, "Balance", false, false}
    };
    noncompatible_schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName"});
    noncompatible_schema.setRangeKeyFieldsByName(std::vector<k2::String>{"FirstName"});

    k2::dto::SKVRecord doc("collection", std::make_shared<k2::dto::Schema>(schema));
    doc.serializeNext<k2::String>("a");
    doc.serializeNext<k2::String>("b");
    doc.serializeNext<int32_t>(12);

    // Should clone without error
    k2::dto::SKVRecord clone = doc.cloneToOtherSchema("collection",
                                    std::make_shared<k2::dto::Schema>(compatible_schema));

    try {
        // Non-compatible schema, should throw exception
        k2::dto::SKVRecord clone = doc.cloneToOtherSchema("collection",
                                    std::make_shared<k2::dto::Schema>(noncompatible_schema));
        REQUIRE(false);
    } catch (...) {}
}
