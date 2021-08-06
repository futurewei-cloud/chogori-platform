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

TEST_CASE("Test3: int32 and NULL key ordering") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::INT32T, "ID", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"ID"});
    std::shared_ptr<k2::dto::Schema> schema_ptr = std::make_shared<k2::dto::Schema>(std::move(schema));

    k2::dto::SKVRecord zero("collection", schema_ptr);
    k2::dto::SKVRecord one("collection", schema_ptr);
    k2::dto::SKVRecord small("collection", schema_ptr);
    k2::dto::SKVRecord big("collection", schema_ptr);
    k2::dto::SKVRecord null("collection", schema_ptr);

    zero.serializeNext<int32_t>(0);
    one.serializeNext<int32_t>(1);
    small.serializeNext<int32_t>(777);
    big.serializeNext<int32_t>(3000000);
    null.serializeNull();

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

TEST_CASE("Test4: signed int key ordering") {
    k2::dto::Schema schema;
    schema.name = "int16";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::INT16T, "ID", false, false},
    };
    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"ID"});
    std::shared_ptr<k2::dto::Schema> schema16 = std::make_shared<k2::dto::Schema>(std::move(schema));

    schema.name = "int32";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::INT32T, "ID", false, false},
    };
    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"ID"});
    std::shared_ptr<k2::dto::Schema> schema32 = std::make_shared<k2::dto::Schema>(std::move(schema));

    schema.name = "int64";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::INT64T, "ID", false, false},
    };
    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"ID"});
    std::shared_ptr<k2::dto::Schema> schema64 = std::make_shared<k2::dto::Schema>(std::move(schema));

    std::vector<std::shared_ptr<k2::dto::Schema>> schemas;
    schemas.push_back(schema16);
    schemas.push_back(schema32);
    schemas.push_back(schema64);

    for (std::shared_ptr<k2::dto::Schema>& s : schemas) {
        k2::dto::SKVRecord smallneg("collection", s);
        k2::dto::SKVRecord largeneg("collection", s);
        k2::dto::SKVRecord zero("collection", s);
        k2::dto::SKVRecord small("collection", s);
        k2::dto::SKVRecord large("collection", s);

        if (s->name == "int16") {
            smallneg.serializeNext<int16_t>(-20);
            largeneg.serializeNext<int16_t>(-1000);
            zero.serializeNext<int16_t>(0);
            large.serializeNext<int16_t>(1001);
            small.serializeNext<int16_t>(21);
        }
        else if (s->name == "int32") {
            smallneg.serializeNext<int32_t>(-20);
            largeneg.serializeNext<int32_t>(-10000);
            zero.serializeNext<int32_t>(0);
            large.serializeNext<int32_t>(10010);
            small.serializeNext<int32_t>(21);
        }
        else if (s->name == "int64") {
            smallneg.serializeNext<int64_t>(-20);
            largeneg.serializeNext<int64_t>(-100000);
            zero.serializeNext<int64_t>(0);
            large.serializeNext<int64_t>(100100);
            small.serializeNext<int64_t>(21);
        }

        k2::String smallneg_s = smallneg.getPartitionKey();
        k2::String largeneg_s = largeneg.getPartitionKey();
        k2::String zero_s = zero.getPartitionKey();
        k2::String large_s = large.getPartitionKey();
        k2::String small_s = small.getPartitionKey();

        REQUIRE(largeneg_s < smallneg_s);
        REQUIRE(smallneg_s < zero_s);
        REQUIRE(zero_s < small_s);
        REQUIRE(small_s < large_s);
    }
}

TEST_CASE("Test5: bool key ordering") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::BOOL, "myBool", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"myBool"});
    std::shared_ptr<k2::dto::Schema> schema_ptr = std::make_shared<k2::dto::Schema>(std::move(schema));

    k2::dto::SKVRecord isfalse("collection", schema_ptr);
    k2::dto::SKVRecord istrue("collection", schema_ptr);
    k2::dto::SKVRecord null("collection", schema_ptr);

    isfalse.serializeNext<bool>(false);
    istrue.serializeNext<bool>(true);
    null.serializeNull();

    k2::String falseKey = isfalse.getPartitionKey();
    k2::String trueKey = istrue.getPartitionKey();
    k2::String nullKey = null.getPartitionKey();

    REQUIRE(nullKey < falseKey);
    REQUIRE(falseKey < trueKey);
}

TEST_CASE("Test6: Mixed type compound keys string int32") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "Name", false, false},
            {k2::dto::FieldType::INT32T, "Value", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"Name", "Value"});
    std::shared_ptr<k2::dto::Schema> schema_ptr = std::make_shared<k2::dto::Schema>(std::move(schema));

    k2::dto::SKVRecord coll1("collection", schema_ptr);
    k2::dto::SKVRecord coll2("collection", schema_ptr);

    coll1.serializeNext<k2::String>("TestName");
    coll1.serializeNext<int32_t>(10000);

    coll2.serializeNext<k2::String>("TestName");
    coll2.serializeNext<int32_t>(10);

    k2::String coll1Key = coll1.getPartitionKey();
    k2::String coll2Key = coll2.getPartitionKey();

    REQUIRE(coll2Key < coll1Key);
}

TEST_CASE("Test7: Mixed type compound keys string bool") {
    k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "Name", false, false},
            {k2::dto::FieldType::BOOL, "myBool", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"Name", "myBool"});
    std::shared_ptr<k2::dto::Schema> schema_ptr = std::make_shared<k2::dto::Schema>(std::move(schema));

    k2::dto::SKVRecord coll1("collection", schema_ptr);
    k2::dto::SKVRecord coll2("collection", schema_ptr);

    coll1.serializeNext<k2::String>("TestName");
    coll1.serializeNext<bool>(false);

    coll2.serializeNext<k2::String>("TestName");
    coll2.serializeNext<bool>(true);

    k2::String coll1Key = coll1.getPartitionKey();
    k2::String coll2Key = coll2.getPartitionKey();

    REQUIRE(coll1Key < coll2Key);
}
