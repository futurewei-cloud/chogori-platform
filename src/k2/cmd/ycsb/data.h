/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include <decimal/decimal>
#include <string>

#include <k2/common/Common.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>

#include <k2/cmd/ycsb/ycsb_rand.h>
#include <k2/cmd/ycsb/Log.h>

using namespace k2;
static const String ycsbCollectionName = "YCSB";

// helper function to convert to base (default 93 (ascii from 33 to 126)) representation
String iToString(int n, int base = 93)
{
    uint32_t len = ceil(std::log(n+1)/std::log(base));
    String buf(String::initialized_later{},len);
    std::div_t dv{}; dv.quot = n; dv.rem = 0;
    uint32_t i = 0;
    do {
        dv = std::div(dv.quot, base);
        buf[i] = (char)(std::abs(dv.rem)+33);  // avoid ascii value 126 because it is used for padding
        i++;
    } while(dv.quot);

    return buf;
}

class YCSBData{

public:

    // function to generate the schema for YCSB Data based on the number of fields per records
    static dto::Schema generate_schema(const uint32_t num_fields){

        std::vector<dto::SchemaField> field_names;

        field_names.reserve(num_fields);

        for(uint32_t field_no = 0; field_no < num_fields; ++field_no){
            String fieldname = "field" + std::to_string(field_no);
            dto::SchemaField f {dto::FieldType::STRING, fieldname , false, false};
            field_names.push_back(f);
        }

        dto::Schema ycsb_data_schema {
            .name = "ycsb_data",
            .version = 1,
            .fields = field_names,
            .partitionKeyFields = std::vector<uint32_t> { 0 },
            .rangeKeyFields = std::vector<uint32_t> {}
        };

        return ycsb_data_schema;
    }

    // function to obtain generate key for a particular id
    static String idToKey(uint64_t keyid, uint32_t len_field){
        String key = iToString(keyid);

        K2ASSERT(log::ycsb,key.size()<=len_field,"key size exceeds field length");

        String padding( (len_field - key.size()),char(126));

        return padding + key;
    }

    YCSBData(uint64_t keyid) : ID(keyid) {}

    YCSBData(uint64_t keyid, RandomContext& random) : ID(keyid) {

        fields.push_back(YCSBData::idToKey(keyid,_len_field()));

        // populate fields with random strings of fixed length
        for(uint32_t field_no = 1; field_no < _num_fields(); ++field_no){
             fields.push_back(random.RandomString(_len_field()));
        }
    }

    uint64_t ID;
    std::vector<String> fields;
    static inline dto::Schema ycsb_schema;
    static inline thread_local std::shared_ptr<dto::Schema> schema; // schema required in this format for creating SKV record
    static inline String collectionName = ycsbCollectionName;

private:
    k2::ConfigVar<uint32_t> _len_field{"field_length"};
    k2::ConfigVar<uint32_t> _num_fields{"num_fields"};
};

// function to write the given YCSB Data row
seastar::future<WriteResult> writeRow(YCSBData& row, K2TxnHandle& txn, bool erase = false)
{
    dto::SKVRecord skv_record(YCSBData::collectionName, YCSBData::schema); // create SKV record

    for(auto&& field : row.fields){
        skv_record.serializeNext<String>(field); // add fields to SKV record
    }

    return txn.write<dto::SKVRecord>(skv_record, erase).then([] (WriteResult&& result) {
        if (!result.status.is2xxOK()) {
            K2LOG_D(log::ycsb, "writeRow failed: {}", result.status);
            return seastar::make_exception_future<WriteResult>(std::runtime_error("writeRow failed!"));
        }

        return seastar::make_ready_future<WriteResult>(std::move(result));
    });
}

// function to update the partial fields for a YCSB Data row
seastar::future<PartialUpdateResult>
partialUpdateRow(std::vector<String> fieldValues, std::vector<String> fieldsToUpdate, K2TxnHandle& txn) {

    dto::SKVRecord skv_record(YCSBData::collectionName, YCSBData::schema); // create SKV record

    for(auto&& field : fieldValues){
        skv_record.serializeNext<String>(field); // add values of fields to be updated to SKV record
    }

    return txn.partialUpdate<dto::SKVRecord>(skv_record, fieldsToUpdate).then([] (PartialUpdateResult&& result) {
        if (!result.status.is2xxOK()) {
            K2LOG_D(log::ycsb, "partialUpdateRow failed: {}", result.status);
            return seastar::make_exception_future<PartialUpdateResult>(std::runtime_error("partialUpdateRow failed!"));
        }

        return seastar::make_ready_future<PartialUpdateResult>(std::move(result));
    });
}

void setupSchemaPointers() {
    YCSBData::schema = std::make_shared<dto::Schema>(YCSBData::ycsb_schema);
}
