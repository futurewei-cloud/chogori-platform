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

TEST_CASE("Test1: Compound string keys with prefix") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});
    std::shared_ptr<k2::dto::Schema> schema_ptr = std::make_shared<k2::dto::Schema>(std::move(schema));

    k2::dto::SKVRecord bob("collection", schema_ptr);
    k2::dto::SKVRecord bo("collection", schema_ptr);

    bob.serializeNext<k2::String>("Bob");
    bob.serializeNext<k2::String>("urns");

    bo.serializeNext<k2::String>("Bo");
    bo.serializeNext<k2::String>("burns");

    k2::String bobKey = bob.getPartitionKey();
    k2::String boKey = bo.getPartitionKey();

    REQUIRE(boKey < bobKey);
}

TEST_CASE("Test2: Strings with NULL bytes") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName"});
    std::shared_ptr<k2::dto::Schema> schema_ptr = std::make_shared<k2::dto::Schema>(std::move(schema));

    k2::dto::SKVRecord doc("collection", schema_ptr);
    k2::String bytes("aaaaaaa");
    bytes[0] = '\0';
    bytes[4] = '\0';
    bytes[5] = '\0';
    doc.serializeNext<k2::String>(std::move(bytes));

    std::vector<uint8_t> expectedKey{
        1, // Type byte
        0, // escape
        255, // escaped null
        97, // 'a'
        97, // 'a'
        97, // 'a'
        0, // escape
        255, // escaped null
        0, // escape
        255, // escaped null
        97, // 'a'
        0, // Terminator
        1, // Terminator
    };
    k2::String partitionKey = doc.getPartitionKey();

    REQUIRE(expectedKey.size() == partitionKey.size());
    for (size_t i = 0; i < partitionKey.size(); ++i) {
        REQUIRE(expectedKey[i] == (uint8_t)partitionKey[i]);
    }
}

TEST_CASE("Test3: uint32 and NULL key ordering") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::UINT32T, "ID", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"ID"});
    std::shared_ptr<k2::dto::Schema> schema_ptr = std::make_shared<k2::dto::Schema>(std::move(schema));

    k2::dto::SKVRecord zero("collection", schema_ptr);
    k2::dto::SKVRecord one("collection", schema_ptr);
    k2::dto::SKVRecord small("collection", schema_ptr);
    k2::dto::SKVRecord big("collection", schema_ptr);
    k2::dto::SKVRecord null("collection", schema_ptr);

    zero.serializeNext<uint32_t>(0);
    one.serializeNext<uint32_t>(1);
    small.serializeNext<uint32_t>(777);
    big.serializeNext<uint32_t>(3000000);
    null.skipNext();

    k2::String zeroKey = zero.getPartitionKey();
    k2::String oneKey = one.getPartitionKey();
    k2::String smallKey = small.getPartitionKey();
    k2::String bigKey = big.getPartitionKey();
    k2::String nullKey = null.getPartitionKey();

    REQUIRE(nullKey < zeroKey);
    REQUIRE(zeroKey < oneKey);
    REQUIRE(oneKey < smallKey);
    REQUIRE(smallKey < bigKey);
}

