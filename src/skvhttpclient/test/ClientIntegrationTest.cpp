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

void testCreateSchema() {
  dto::Schema schema {
    .name = schemaName,
    .version = 1,
    .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::STRING, "FirstName", false, false},
            {dto::FieldType::INT32T, "Balance", false, false}
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
  builder.serializeNull();
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

int main() {
  testCreateCollection();
  testCreateSchema();
  // Tests getSchema, beginTxn, write, and endTxn
  testBasicWritePath();
  testRead();
  
  return 0;
}
