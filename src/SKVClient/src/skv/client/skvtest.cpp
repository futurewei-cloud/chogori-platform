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
#include <iostream>
#include <string>
#include "SKVClient.h"

using namespace skv::http;

std::shared_ptr<dto::Schema> getSchema() {
    static std::shared_ptr<dto::Schema> schemaPtr;
    if (!schemaPtr) {
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
        schemaPtr = std::make_shared<dto::Schema>(schema);
    }
    return schemaPtr;
}

bool basicTxnTest(Client& client) {
    K2LOG_I(log::dto, "Sending begin txn");
    auto result = client.beginTxn(dto::TxnOptions{})
        .then([] (auto&& fut) {
            // Get txn handle
            auto&& [status, txn]  = fut.get();
            K2LOG_I(log::dto, "Got txn handle {}", status);
            K2ASSERT(log::dto, status.is2xxOK(), "Begin txn failed");
            return std::move(txn);
        })
        .then([] (auto&& fut) {
            // Write
            TxnHandle txn  = fut.get();
            dto::SKVRecordBuilder builder("test_collection", getSchema());
            K2LOG_I(log::dto, "Writing b, a, 12");
            builder.serializeNext<String>("b");
            builder.serializeNext<String>("a");
            builder.serializeNext<int32_t>(12);
            // Pack txn with future of wrjite result and return
            return std::make_tuple(std::move(txn),
                txn.write(builder.build()));
        })
        .then([] (auto&& fut) {
            //  Process wriite
            auto&& [txn, innerfut] = fut.get();
            auto&& [status] = innerfut.get();
            K2ASSERT(log::dto, status.is2xxOK(), "write failed");
            return std::move(txn);
        })
        .then([] (auto&& fut) {
            // Read
            TxnHandle txn  = fut.get();
            dto::SKVRecordBuilder builder("test_collection", getSchema());
            builder.serializeNext<String>("b");
            builder.serializeNext<String>("a");
            // Pack txn with read result
            return std::make_tuple(std::move(txn), txn.read(builder.build()));
        })
        .then([] (auto&& fut) {
            //  Process read
            auto&& [txn, resfut] = fut.get();
            auto&& [status, rec] = resfut.get();
            K2ASSERT(log::dto, status.is2xxOK(), "read failed");
            K2LOG_I(log::dto, "Read result {}", rec);
            rec.seekField(0);
            std::optional<String> last = rec.template deserializeNext<String>();
            K2ASSERT(log::dto, last == "b", "lastname");
            std::optional<String> first = rec.template deserializeNext<String>();
            K2ASSERT(log::dto, first == "a", "firstname");
            std::optional<int32_t> balance = rec.template deserializeNext<int32_t>();
            K2ASSERT(log::dto, balance == 12, "balance");
            K2LOG_I(log::dto, "Read got record {}, {}, {}", last, first, balance);
            return txn.endTxn(true);
        })
        .then([] (auto&& fut) {
            // Commit
            auto&& [status] = fut.get().get();
            K2ASSERT(log::dto, status.is2xxOK(), "End txn failed");
            K2LOG_I(log::dto, "End txn ends {}", status);

            return true;
        }).get();
    return result;
}

int main() {
    skv::http::Client client;
    auto result = basicTxnTest(client);
    return result ? 0 : 1;
}
