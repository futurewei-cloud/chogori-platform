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

#include <cstdarg>
#include <k2/dto/SKVRecord.h>

#include "catch2/catch.hpp"

template <typename T>
void Compare(std::optional<T> value, const k2::String& fieldName, ...);

template <>
void Compare<k2::String>(std::optional<k2::String> value, const k2::String& fieldName, ...) {
	va_list argPtr;
	va_start(argPtr, fieldName);
	k2::dto::SKVRecord* record = va_arg(argPtr, k2::dto::SKVRecord*);

	if (value == std::nullopt) {  // deal with fieldCursor for NULL Field deserialize		
		k2::String cursorfield = record->schema.fields[record->fieldCursor].name;
		if (cursorfield != "FirstName" && cursorfield != "Job") {
			REQUIRE(false);
		}		
		record->fieldCursor ++;		
	}
	else if (fieldName == "LastName") {
		REQUIRE(*value == "Baggins");		
	}
	else {
		REQUIRE(false);
	}
	va_end(argPtr);
}

template <>
void Compare<uint32_t>(std::optional<uint32_t> value, const k2::String& fieldName, ...) {
	if (fieldName == "Balance") {
		REQUIRE(*value == 100);
	}
	else if (fieldName == "Age") {
		REQUIRE(*value == 36);
	}
	else {
		REQUIRE(false);
	}	
}



// Serialize a record with composite partition and range keys 
// (e.g. partition key is string field + uint32_t field + string field) 
TEST_CASE("Test1: Serialize a record with composite partition and range keys") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::UINT32T, "UserId", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
            {k2::dto::FieldType::STRING, "Job", false, false},
            {k2::dto::FieldType::UINT32T, "Age", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "UserId", "FirstName"});
    schema.setRangeKeyFieldsByName(std::vector<k2::String>{"Balance", "Job", "Age"});

	k2::dto::SKVRecord doc("collection", std::move(schema));

	doc.serializeNext<k2::String>("Baggins");
	doc.serializeNext<uint32_t>(20201234);
    doc.serializeNext<k2::String>("Bilbo");
	doc.serializeNext<uint32_t>(100);
    doc.serializeNext<k2::String>("Teacher");
	doc.serializeNext<uint32_t>(36);

	std::vector<uint8_t> expectedPKey {
		1, // Type string
		66, 97, 103, 103, 105, 110, 115, // B,a,g,g,i,n,s
		0, // ESCAPE
		1, // TERM
		2, // Type u32
		1, 52, 63, 18, // 20201234
		0, // ESCAPE
		1, // TERM
		1, // Type string
		66, 105, 108, 98, 111, // B,i,l,b,o
		0, // ESCAPE
		1, // TERM
	};
	k2::String partitionKey = doc.getPartitionKey();
	REQUIRE(expectedPKey.size() == partitionKey.size());
	for (size_t i = 0; i < partitionKey.size(); ++i) {
		REQUIRE(expectedPKey[i] == (uint8_t)partitionKey[i]);
	}
	std::cout << "Test1: All encoded Partition Key matches." << std::endl;

	std::vector<uint8_t> expectedRKey {
		2, // Type u32
		0, 0, 0, 100, // 100
		0, // ESCAPE
		1, // TERM 
		1, // Type string
		84, 101, 97, 99, 104, 101, 114, // T,e,a,c,h,e,r
		0, // ESCAPE
		1, // TERM
		2, // Type u32
		0, 0, 0, 36, // 36
		0, // ESCAPE
		1, // TERM
	};
	k2::String rangeKey = doc.getRangeKey();
	REQUIRE(expectedRKey.size() == rangeKey.size());
	for (size_t i = 0; i < rangeKey.size(); ++i) {
		REQUIRE(expectedRKey[i] == (uint8_t)rangeKey[i]);
	}
	std::cout << "Test1: All encoded Range Key matches." << std::endl;
}


TEST_CASE("Test2: Serialize a record with a composite partition key and one key field NULL") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::UINT32T, "UserId", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
            {k2::dto::FieldType::STRING, "Job", false, false},
            {k2::dto::FieldType::UINT32T, "Age", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "UserId", "FirstName", "Balance"});

	k2::dto::SKVRecord doc("collection", std::move(schema));
	
	doc.serializeNext<k2::String>("Baggins");
	doc.serializeNext<uint32_t>(20201234);
	doc.skipNext();
	doc.skipNext();
	
	std::vector<uint8_t> expectedPKey {
		1, // Type string
		66, 97, 103, 103, 105, 110, 115, // B,a,g,g,i,n,s
		0, // ESCAPE
		1, // TERM
		2, // Type u32
		1, 52, 63, 18, // 20201234
		0, // ESCAPE
		1, // TERM
		0, // Type NULL_T
		0, // ESCAPE
		1, // TERM
		0, // Type NULL_T
		0, // ESCAPE
		1, // TERM
	};
	k2::String partitionKey = doc.getPartitionKey();
	REQUIRE(expectedPKey.size() == partitionKey.size());
	for (size_t i = 0; i < partitionKey.size(); ++i) {
		REQUIRE(expectedPKey[i] == (uint8_t)partitionKey[i]);
	}
	std::cout << "Test2: Composite partition key with key field NULL encoding success." << std::endl;
}


TEST_CASE("Test3: Serialize a record with a composite partition key and one key field (designated NullLast) is NULL ") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::UINT32T, "UserId", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::UINT32T, "Balance", false, true},
            {k2::dto::FieldType::STRING, "Job", false, false},
            {k2::dto::FieldType::UINT32T, "Age", false, false}
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "UserId", "FirstName", "Balance"});

	k2::dto::SKVRecord doc("collection", std::move(schema));
	
	doc.serializeNext<k2::String>("Baggins");
	doc.serializeNext<uint32_t>(20201234);
	doc.skipNext();
	doc.skipNext();
	
	std::vector<uint8_t> expectedPKey {
		1, // Type string
		66, 97, 103, 103, 105, 110, 115, // B,a,g,g,i,n,s
		0, // ESCAPE
		1, // TERM
		2, // Type u32
		1, 52, 63, 18, // 20201234
		0, // ESCAPE
		1, // TERM
		0, // Type NULL_T
		0, // ESCAPE
		1, // TERM
		255, // Type NULL_Last
		0, // ESCAPE
		1, // TERM
	};
	k2::String partitionKey = doc.getPartitionKey();
	REQUIRE(expectedPKey.size() == partitionKey.size());
	for (size_t i = 0; i < partitionKey.size(); ++i) {
		REQUIRE(expectedPKey[i] == (uint8_t)partitionKey[i]);
	}
	std::cout << "Test3: Composite partition key with key field NullLast encoding success." << std::endl;
}


TEST_CASE("Test4: Serialize a record with one value field skipped") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, true},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
            {k2::dto::FieldType::STRING, "Job", false, false},
            {k2::dto::FieldType::UINT32T, "Age", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});

	k2::dto::SKVRecord doc("collection", std::move(schema));
	
	doc.serializeNext<k2::String>("Baggins");
	doc.skipNext();
	doc.serializeNext<uint32_t>(100);
	doc.skipNext();
	doc.serializeNext<uint32_t>(36);

	// deserialize using "deserializeNext" function
	doc.seekField(0);
	std::optional<k2::String> lastName = doc.deserializeNext<k2::String>();
	REQUIRE(*lastName == "Baggins");
	std::optional<k2::String> firstName = doc.deserializeNext<k2::String>();
	REQUIRE(firstName == std::nullopt);
	doc.fieldCursor ++;		// have to move fieldCursor to chase the field
	std::optional<uint32_t> balance = doc.deserializeNext<uint32_t>();
    REQUIRE(*balance == 100);
	std::optional<k2::String> job = doc.deserializeNext<k2::String>();
	REQUIRE(job == std::nullopt);
	doc.fieldCursor ++;
	std::optional<uint32_t> age = doc.deserializeNext<uint32_t>();
	REQUIRE(*age == 36);
	std::cout << "Test4: Deserialize by \"deserializeNext\" function success." << std::endl;

	// deserialize using "FOR_EACH_RECORD_FIELD" macro
	FOR_EACH_RECORD_FIELD(doc, Compare, &doc);
	std::cout << "Test4: Deserialize by \"FOR_EACH_RECORD_FIELD\" macro success." << std::endl;
	
	std::cout << "Test4: Fields deserialized successfully using the deserializeNextOptional function and with the FOR_EACH_RECORD_FIELD macro." << std::endl;
}


TEST_CASE("Test5: Deserialize fields out of order by name") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, true},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
            {k2::dto::FieldType::STRING, "Job", false, false},
            {k2::dto::FieldType::UINT32T, "Age", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});

	k2::dto::SKVRecord doc("collection", std::move(schema));
	
	doc.serializeNext<k2::String>("Baggins");
	doc.skipNext();
	doc.serializeNext<uint32_t>(100);
	doc.skipNext();
	doc.serializeNext<uint32_t>(36);

	// out of order Deserialize, using "deserializeFiled(String&)" function
	doc.seekField(2);
	std::optional<uint32_t> balance = doc.deserializeField<uint32_t>("Balance");
	REQUIRE(*balance == 100);
	
	doc.seekField(3);
	std::optional<k2::String> job = doc.deserializeField<k2::String>("Job");
	REQUIRE(job == std::nullopt);
	
	doc.seekField(4);
	std::optional<uint32_t> age = doc.deserializeField<uint32_t>("Age");
	REQUIRE(*age == 36);

	doc.seekField(0);
	std::optional<k2::String> lastName = doc.deserializeField<k2::String>("LastName");
	REQUIRE(*lastName == "Baggins");

	doc.seekField(1);
	std::optional<k2::String> firstName = doc.deserializeField<k2::String>("FirstName");
	REQUIRE(firstName == std::nullopt);

	std::cout << "Test5: Deserialize fields out of order by name success." << std::endl;
}


// Error test cases

TEST_CASE("Test6: getPartitionKey() is called before all partition key fields are serialized") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});
    schema.setRangeKeyFieldsByName(std::vector<k2::String>{"Balance"});

	k2::dto::SKVRecord doc("collection", std::move(schema));

	doc.serializeNext<k2::String>("Baggins");

	try {
		k2::String partitionKey = doc.getPartitionKey();
		REQUIRE(false);
    } catch (...) {
		std::cout << "Test6: Partition key field not set." << std::endl;
	}
}


TEST_CASE("Test7: getRangeKey() is called before all range key fields are serialized") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});
    schema.setRangeKeyFieldsByName(std::vector<k2::String>{"Balance"});

	k2::dto::SKVRecord doc("collection", std::move(schema));

	try {
		k2::String rangeKey = doc.getRangeKey();
		REQUIRE(false);
    } catch (...) {
		std::cout << "Test7: Range key field not set." << std::endl;
	}
}


TEST_CASE("Test8: deserializeField(string name) on a name that is not in schema") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, true},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});

	k2::dto::SKVRecord doc("collection", std::move(schema));
	
	doc.serializeNext<k2::String>("Baggins");
	doc.serializeNext<k2::String>("Bilbo");
	doc.serializeNext<uint32_t>(100);

	try {	// using "deserializeFiled(String&)" function with a wrong name
		doc.seekField(0);
		std::optional<k2::String> lastName = doc.deserializeField<k2::String>("Job");
		REQUIRE(false);
	} catch (...) {
		std::cout << "Test8: Deserialize name string is not in schema." << std::endl;
	}
}


TEST_CASE("Test9: seekField() with a field index out-of-bounds for the schema") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, true},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});

	k2::dto::SKVRecord doc("collection", std::move(schema));
	
	doc.serializeNext<k2::String>("Baggins");
	doc.serializeNext<k2::String>("Bilbo");
	doc.serializeNext<uint32_t>(100);
	doc.seekField(0);

	try {
		doc.seekField(3);
		REQUIRE(false);
	} catch (...) {
		std::cout << "Test9: Tried to seek outside bounds." << std::endl;
	}
}


TEST_CASE("Test10: Deserialize a field that has not been serialized for the document") {
	k2::dto::Schema schema;
    schema.name = "test_schema";
    schema.version = 1;
    schema.fields = std::vector<k2::dto::SchemaField> {
            {k2::dto::FieldType::STRING, "LastName", false, false},
            {k2::dto::FieldType::STRING, "FirstName", false, false},
            {k2::dto::FieldType::UINT32T, "Balance", false, false},
    };

    schema.setPartitionKeyFieldsByName(std::vector<k2::String>{"LastName", "FirstName"});

	k2::dto::SKVRecord doc("collection", std::move(schema));
	
	try {
		// deserialize a field which has not been serialized
		std::optional<k2::String> lastName = doc.deserializeNext<k2::String>();
		REQUIRE(*lastName == "");
		throw new std::runtime_error("deserialize a non-serialized field");
	} catch (...) {
		std::cout << "Test10: Deserialize a field which has not been serialized." << std::endl;
	}
}


