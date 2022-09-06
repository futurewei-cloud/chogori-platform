/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

#include "skvhttp/dto/ControlPlaneOracle.h"
#include "skvhttp/dto/K23SI.h"
#include <skvhttp/dto/SKVRecord.h>
#include <string>

#include <k2/logging/Log.h>
#include <skvhttp/common/Common.h>
#include <skvhttp/dto/Collection.h>
#include <skvhttp/client/SKVClient.h>

namespace k2::log {
  inline thread_local k2::logging::Logger httpclient("k2::httpclient_test");
}

using namespace skv::http;

Client client;
const std::string collectionName = "k23si_test_collection";
const std::string schemaName = "test_schema";

// Helper functions
// Verify that two records are equal, the parameters are not const as deserialize is not const function
void verifyEqual(dto::SKVRecord& first, dto::SKVRecord& second) {
    first.seekField(0);
    second.seekField(0);
    K2EXPECT(k2::log::httpclient, first.schema, second.schema);
    first.visitRemainingFields([&second] (const auto& field, auto val1) {
        auto val2 = second.deserializeField<typename decltype(val1)::value_type>(field.name);
        K2EXPECT(k2::log::httpclient, val1.has_value(), val2.has_value());
        if (val1.has_value()) {
            K2EXPECT(k2::log::httpclient, val1.value(), val2.value());
        }
    });
    first.seekField(0);
    second.seekField(0);
}

// Compare a query result with reference records
void verifyEqual(std::vector<dto::SKVRecord>& first, std::vector<dto::SKVRecord>&& second) {
    K2EXPECT(k2::log::httpclient, first.size(), second.size());
    for (size_t i=0; i < first.size(); i++) {
        verifyEqual(first[i], second[i]);
    }
}

dto::SKVRecordBuilder& serialize(dto::SKVRecordBuilder& builder) {return builder;}

// Get a builder by serializing args
template<typename Second, typename... Args>
dto::SKVRecordBuilder& serialize(dto::SKVRecordBuilder& builder, Second&& second, Args&&... args) {
    builder.serializeNext<Second>(std::forward<Second>(second));
    return serialize(builder, std::forward<Args>(args)...);
}

// Get a SKV record by serializing args
template<typename... Args>
dto::SKVRecord buildRecord(const std::string& collectionName, std::shared_ptr<dto::Schema> schemaPtr, Args&&... args) {
    dto::SKVRecordBuilder builder(collectionName, schemaPtr);
    return serialize(builder, args...).build();
}

// Populate initial data
void writeRecord(dto::SKVRecord& record) {
    auto&& [beginStatus, txn] = client.beginTxn(dto::TxnOptions{}).get();
    K2EXPECT(k2::log::httpclient, beginStatus.is2xxOK(), true);

    auto&& [writeStatus] = txn.write(record).get();
    K2EXPECT(k2::log::httpclient, writeStatus.is2xxOK(), true);

    auto&& [endStatus] = txn.endTxn(dto::EndAction::Commit).get();
    K2EXPECT(k2::log::httpclient, endStatus.is2xxOK(), true);
}

// Verify that an exception is thrown
template <typename Exception, typename Fn>
void expectException(Fn&& fn) {
    try {fn();} catch (Exception& e){return;}
    K2EXPECT(k2::log::httpclient, true, false);
}

void testCreateCollection() {
  dto::CollectionMetadata metadata {
    .name = collectionName,
    .hashScheme = dto::HashScheme::Range,
    .storageDriver = dto::StorageDriver::K23SI,
    .capacity = dto::CollectionCapacity{},
    .retentionPeriod = Duration(0),
    .heartbeatDeadline = Duration(0),
    .deleted = false
  };
  std::vector<std::string> rangeEnds{""};
  auto&& [status] = client.createCollection(std::move(metadata), std::move(rangeEnds)).get();
  K2LOG_I(k2::log::httpclient, "crateCollection respone status: {}", status);
  K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);
}

void testCreateHashCollection() {
  dto::CollectionMetadata metadata {
    .name = collectionName,
    .hashScheme = dto::HashScheme::HashCRC32C,
    .storageDriver = dto::StorageDriver::K23SI,
    .capacity = dto::CollectionCapacity{},
    .retentionPeriod = Duration(0),
    .heartbeatDeadline = Duration(0),
    .deleted = false
  };
  auto&& [status] = client.createCollection(std::move(metadata), {}).get();
  K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);
}

void testCreateSchema() {
  dto::Schema schema {
    .name = schemaName,
    .version = 1,
    .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::STRING, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false},
            {dto::FieldType::STRING, "Data", false, false},
    },
    .partitionKeyFields = std::vector<uint32_t>(),
    .rangeKeyFields = std::vector<uint32_t>(),
  };

    schema.setPartitionKeyFieldsByName(std::vector<String>{"LastName"});
    schema.setRangeKeyFieldsByName(std::vector<String>{"FirstName"});

    auto&& [status] = client.createSchema(collectionName, schema).get();
    K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);
}

// Tests getSchema, beginTxn, write, and endTxn
void testBasicWritePath() {
  auto&& [status, schemaPtr] = client.getSchema(collectionName, schemaName, 1).get();
  K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);

  auto&& [beginStatus, txn] = client.beginTxn(dto::TxnOptions{}).get();
  K2EXPECT(k2::log::httpclient, beginStatus.is2xxOK(), true);

  dto::SKVRecordBuilder builder(collectionName, schemaPtr);
  builder.serializeNext<std::string>("A");
  builder.serializeNext<std::string>("B");
  builder.serializeNext<int32_t>(33);
  builder.serializeNext<std::string>("data");

  dto::SKVRecord record = builder.build();
  auto&& [writeStatus] = txn.write(record).get();
  K2EXPECT(k2::log::httpclient, writeStatus.is2xxOK(), true);

  auto&& [endStatus] = txn.endTxn(dto::EndAction::Commit).get();
  K2EXPECT(k2::log::httpclient, endStatus.is2xxOK(), true);
}

void testRead() {
  auto&& [status, schemaPtr] = client.getSchema(collectionName, schemaName).get();
  K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);

  auto&& [beginStatus, txn] = client.beginTxn(dto::TxnOptions{}).get();
  K2EXPECT(k2::log::httpclient, beginStatus.is2xxOK(), true);

  dto::SKVRecordBuilder builder(collectionName, schemaPtr);
  builder.serializeNext<std::string>("A");
  builder.serializeNext<std::string>("B");
  auto&& [readStatus, readRecord] = txn.read(builder.build()).get();
  K2EXPECT(k2::log::httpclient, readStatus.is2xxOK(), true);

  std::string a = readRecord.deserializeNext<std::string>().value();
  K2EXPECT(k2::log::httpclient, a, "A");
  std::string b = readRecord.deserializeNext<std::string>().value();
  K2EXPECT(k2::log::httpclient, b, "B");
  int32_t c = readRecord.deserializeNext<int32_t>().value();
  K2EXPECT(k2::log::httpclient, c, 33);

  auto&& [endStatus] = txn.endTxn(dto::EndAction::Commit).get();
  K2EXPECT(k2::log::httpclient, endStatus.is2xxOK(), true);
}

std::vector<dto::SKVRecord> queryAll(TxnHandle& txn, const std::string& collectionName, std::shared_ptr<dto::Schema> schemaPtr, dto::QueryRequest& query) {
    bool done = false;
    std::vector<dto::SKVRecord> records;
    while (!done) {
        auto&& [queryStatus, result] = txn.query(query).get();
        K2EXPECT(k2::log::httpclient, queryStatus.is2xxOK(), true);
        done = result.done;
        for (dto::SKVRecord::Storage& storage : result.records) {
            dto::SKVRecord record(collectionName, schemaPtr, std::move(storage), true);
            records.push_back(std::move(record));
        }
    }
    return records;
}

void testQuery() {
    auto&& [status, schemaPtr] = client.getSchema(collectionName, schemaName).get();
    K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);

    auto&& [beginStatus, txn] = client.beginTxn(dto::TxnOptions{}).get();
    K2EXPECT(k2::log::httpclient, beginStatus.is2xxOK(), true);

    dto::SKVRecordBuilder startBuilder(collectionName, schemaPtr);
    dto::SKVRecord start = startBuilder.build();
    dto::SKVRecordBuilder endBuilder(collectionName, schemaPtr);
    dto::SKVRecord end = endBuilder.build();

    auto&& [createStatus, query] = txn.createQuery(start, end).get();
    K2LOG_I(k2::log::httpclient, "crateQuery respone status: {}", createStatus);
    K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);

    bool done = false;
    std::vector<dto::SKVRecord::Storage> records;
    while (!done) {
        auto&& [queryStatus, result] = txn.query(query).get();
        K2EXPECT(k2::log::httpclient, queryStatus.is2xxOK(), true);
        done = result.done;
        for (dto::SKVRecord::Storage& record : result.records) {
            records.push_back(std::move(record));
        }
    }

    K2EXPECT(k2::log::httpclient, records.size(), 1);
    dto::SKVRecord record(collectionName, schemaPtr, std::move(records.front()), true);

    std::string a = record.deserializeNext<std::string>().value();
    K2EXPECT(k2::log::httpclient, a, "A");
    std::string b = record.deserializeNext<std::string>().value();
    K2EXPECT(k2::log::httpclient, b, "B");
    int32_t c = record.deserializeNext<int32_t>().value();
    K2EXPECT(k2::log::httpclient, c, 33);

    auto&& [endStatus] = txn.endTxn(dto::EndAction::Commit).get();
    K2EXPECT(k2::log::httpclient, endStatus.is2xxOK(), true);
}

namespace dtoe = dto::expression;

void testQuery1() {
    auto&& [status, schemaPtr] = client.getSchema(collectionName, schemaName).get();
    K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);

    // Records to compare with, record1 already populated by previous tests
    auto record1 = buildRecord(collectionName, schemaPtr, std::string("A"), std::string("B"), int32_t(33), std::string("data"));
    auto record2 = buildRecord(collectionName, schemaPtr, std::string("A"), std::string("D"), int32_t(34), std::string("data2"));
    writeRecord(record2);

    auto start = buildRecord(collectionName, schemaPtr);
    auto end = buildRecord(collectionName, schemaPtr);

    auto&& [beginStatus, txn] = client.beginTxn(dto::TxnOptions{}).get();
    K2EXPECT(k2::log::httpclient, beginStatus.is2xxOK(), true);

    {
        // Query all, should return record 1 and 2
        auto&& [createStatus, query] = txn.createQuery(start, end).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record1, record2});
    }
    auto record_mid = buildRecord(collectionName, schemaPtr, std::string("A"), std::string("C"), int32_t(33), std::string("data"));
    {
        // Query from range key = C, should only return record 2
        auto&& [createStatus, query] = txn.createQuery(record_mid, end).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record2});
    }
    {
        // Query ending range key = C, should only return record 1
        auto&& [createStatus, query] = txn.createQuery(start, record_mid).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record1});
    }
    {
        // Limit = 1
        auto&& [createStatus, query] = txn.createQuery(start, end, {}, {}, 1).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record1});
    }
    {
        // Reverse = true
        auto&& [createStatus, query] = txn.createQuery(start, end, {}, {}, -1, true).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record2, record1});
    }
    {
        // Limit 1 from reverse
        auto&& [createStatus, query] = txn.createQuery(start, end, {}, {}, 1, true).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record2});
    }
    {
        // Search by prefix = A
        auto prefix = buildRecord(collectionName, schemaPtr, std::string("A"));
        auto&& [createStatus, query] = txn.createQuery(prefix, end).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record1, record2});
    }
    {
        // Query using filter
        dtoe::Expression filter{dtoe::Operation::EQ, {dtoe::makeValueReference("Balance"), dtoe::makeValueLiteral<int32_t>(34)}, {}};
        auto&& [createStatus, query] = txn.createQuery(start, end, std::move(filter)).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        verifyEqual(records, {record2});
    }
    {
        // Query using projection
        auto&& [createStatus, query] = txn.createQuery(start, end, {}, {"LastName", "FirstName"}).get();
        K2EXPECT(k2::log::httpclient, createStatus.is2xxOK(), true);
        auto records = queryAll(txn, collectionName, schemaPtr, query);
        K2EXPECT(k2::log::httpclient, records.size(), 2);
        for (size_t i=0; i < records.size(); i++) {
            auto pkey = records[i].deserializeNext<std::string>();
            K2EXPECT(k2::log::httpclient, *pkey, "A");
            auto rkey = records[i].deserializeNext<std::string>();
            auto expected = i == 0 ? "B" : "D";
            K2EXPECT(k2::log::httpclient, *rkey, expected);
            auto data = records[i].deserializeNext<int32_t>();
            K2EXPECT(k2::log::httpclient, (bool)data, false);
        }
    }

    auto&& [endStatus1] = txn.endTxn(dto::EndAction::Commit).get();
    K2EXPECT(k2::log::httpclient, endStatus1.is2xxOK(), true);

}

void testPartialUpdate() {
    auto&& [status, schemaPtr] = client.getSchema(collectionName, schemaName, 1).get();
    K2EXPECT(k2::log::httpclient, status.is2xxOK(), true);

    auto&& [beginStatus, txn] = client.beginTxn(dto::TxnOptions{}).get();
    K2EXPECT(k2::log::httpclient, beginStatus.is2xxOK(), true);
    auto record = buildRecord(collectionName, schemaPtr, std::string("A1"), std::string("B1"), 33, std::string("Test1"));
    {
      auto&& [writeStatus] = txn.write(record).get();
      K2EXPECT(k2::log::httpclient, writeStatus.is2xxOK(), true);
    }
    auto key = buildRecord(collectionName, schemaPtr, std::string("A1"), std::string("B1"));
    {
        auto&& [readStatus, readRecord] = txn.read(key).get();
        K2EXPECT(k2::log::httpclient, readStatus.is2xxOK(), true);
        verifyEqual(readRecord, record);
    }
    {
        auto record1 = buildRecord(collectionName, schemaPtr, std::string("A1"), std::string("B1"), 34, std::string("Test1_Update"));
        std::vector<uint32_t> fields = {3}; // Update data field only
        auto&& [writeStatus] = txn.partialUpdate(record1, fields).get();
        K2EXPECT(k2::log::httpclient, writeStatus.is2xxOK(), true);
        auto&& [readStatus, readRecord] = txn.read(key).get();
        K2EXPECT(k2::log::httpclient, readStatus.is2xxOK(), true);
        auto compareRecord = buildRecord(collectionName, schemaPtr, std::string("A1"), std::string("B1"), 33, std::string("Test1_Update"));
        verifyEqual(readRecord, compareRecord);
    }
    {
        // Write a record with no value for Data field
        auto record1 = buildRecord(collectionName, schemaPtr, std::string("A1"), std::string("B1"), 34);
        auto&& [writeStatus] = txn.write(record1).get();
        K2EXPECT(k2::log::httpclient, writeStatus.is2xxOK(), true);
        auto&& [readStatus, readRecord] = txn.read(key).get();
        K2EXPECT(k2::log::httpclient, readStatus.is2xxOK(), true);
        verifyEqual(readRecord, record1);

    }
    auto&& [endStatus] = txn.endTxn(dto::EndAction::Commit).get();
    K2EXPECT(k2::log::httpclient, endStatus.is2xxOK(), true);
}

void testValidation() {
  auto&& [schemaStatus, schemaPtr] = client.getSchema(collectionName, schemaName, 1).get();
  K2EXPECT(k2::log::httpclient, schemaStatus.is2xxOK(), true);

  auto&& [beginStatus, txn] = client.beginTxn(dto::TxnOptions{}).get();
  K2EXPECT(k2::log::httpclient, beginStatus.is2xxOK(), true);

  {
      // Bad collection name
      auto record = buildRecord(collectionName + "1", schemaPtr, std::string("A"), std::string("B"), int32_t(33), std::string("data1"));
      auto&& [writeStatus] = txn.write(record).get();
      K2EXPECT(k2::log::httpclient, writeStatus.code, 404);
  }
  {
      // Get bad shema
      auto&& [status, ingoreSchema] = client.getSchema(collectionName, schemaName + "1", 1).get();
      K2EXPECT(k2::log::httpclient, status.code, 404);
  }
  {
      // Bad schema version
      auto&& [status, ignoreSchema] = client.getSchema(collectionName, schemaName, 2).get();
      K2EXPECT(k2::log::httpclient, status.code, 404);
  }
  // Bad partition key type
  expectException<dto::TypeMismatchException>([schemaPtr] {
      buildRecord(collectionName, schemaPtr, int32_t(33), std::string("B"), int32_t(33));
  });
  // Bad range key type
  expectException<dto::TypeMismatchException>([schemaPtr] {
      buildRecord(collectionName, schemaPtr, std::string("A"), int32_t(1), int32_t(33));
  });
  {
      // Good parameter, should succeed
      auto record = buildRecord(collectionName, schemaPtr, std::string("A"), std::string("B"), int32_t(33), std::string("data1"));
      auto&& [writeStatus] = txn.write(record).get();
      K2EXPECT(k2::log::httpclient, writeStatus.is2xxOK(), true);
  }
  auto&& [endStatus] = txn.endTxn(dto::EndAction::Commit).get();
  K2EXPECT(k2::log::httpclient, endStatus.is2xxOK(), true);

}

void testReadWriteConflict() {
  auto&& [schemaStatus, schemaPtr] = client.getSchema(collectionName, schemaName, 1).get();
  K2EXPECT(k2::log::httpclient, schemaStatus.is2xxOK(), true);
  auto record = buildRecord(collectionName, schemaPtr, std::string("ATest"), std::string("BTest"), int32_t(10), std::string("data1"));

  // Populate initial data
  writeRecord(record);

  auto&& [beginStatus1, txn1] = client.beginTxn(dto::TxnOptions{}).get();
  K2EXPECT(k2::log::httpclient, beginStatus1.is2xxOK(), true);

  auto&& [beginStatus2, txn2] = client.beginTxn(dto::TxnOptions{}).get();
  K2EXPECT(k2::log::httpclient, beginStatus2.is2xxOK(), true);

  {
      // Txn2 read
      auto&& [readStatus, readRecord] = txn2.read(record.getSKVKeyRecord()).get();
      K2EXPECT(k2::log::httpclient, readStatus.is2xxOK(), true);
      verifyEqual(readRecord, record);
  }
  {
      // Txn1 try to modify, should get 403
      auto&& [writeStatus] = txn1.write(record).get();
      K2EXPECT(k2::log::httpclient, writeStatus.code, 403);
      // Txn1 commit, should get the same error
      auto&& [endStatus] = txn1.endTxn(dto::EndAction::Commit).get();
      K2EXPECT(k2::log::httpclient, endStatus.code, 403);
  }
  {
      // Txn2 commit, should succeed
      auto&& [endStatus] = txn2.endTxn(dto::EndAction::Commit).get();
      K2EXPECT(k2::log::httpclient, endStatus.is2xxOK(), true);
  }
}

int main() {
  testCreateCollection();
  testCreateHashCollection();
  testCreateSchema();
  // Tests getSchema, beginTxn, write, and endTxn
  testBasicWritePath();
  testRead();
  testQuery();
  testQuery1();
  testPartialUpdate();
  testValidation();
  testReadWriteConflict();

  return 0;
}
