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
// possible operations in YCSB
K2_DEF_ENUM(Operation,
    Read,
    Update,
    Scan,
    Insert,
    Delete
);

#define CHECK_READ_STATUS(read_result) \
    do { \
        if ((read_result).status.code == 404) { \
            K2LOG_D(log::ycsb, "YCSB failed to find keys: {}", (read_result).status); \
            return seastar::make_ready_future(); \
        } \
        else if (!((read_result).status.is2xxOK())) { \
            K2LOG_D(log::ycsb, "YCSB failed to read rows: {}", (read_result).status); \
            return seastar::make_exception_future(std::runtime_error(String("YCSB failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

// helper function to convert to base (default 93 (ascii from 33 to 126)) representation
String iToString(uint64_t  n, uint8_t  base = 93)
{
    uint32_t len = ceil(std::log1p(n)/std::log(base));
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
    static void generateSchema(const uint32_t num_fields){

        std::vector<dto::SchemaField> field_names;

        YCSBData::_fieldNames.reserve(num_fields); // static variable of type vector<String> to store fieldnames
        field_names.reserve(num_fields);

        for(uint32_t field_no = 0; field_no < num_fields; ++field_no){
            String fieldname = "field" + std::to_string(field_no);
            YCSBData::_fieldNames.push_back(fieldname);
            dto::SchemaField f {dto::FieldType::STRING, fieldname , false, false};
            field_names.push_back(f);
        }

        YCSBData::ycsb_schema = dto::Schema{
            .name = "ycsb_data",
            .version = 1,
            .fields = std::move(field_names),
            .partitionKeyFields = std::vector<uint32_t>{0},
            .rangeKeyFields = std::vector<uint32_t>{}
        };

        YCSBData::ycsb_metadata_schema = dto::Schema{
            .name = "ycsb_metadata",
            .version = 1,
            .fields = std::vector<dto::SchemaField>{
                {dto::FieldType::STRING, "key", false, false},
                {dto::FieldType::BOOL, "schemasLoaded", false, false}},
            .partitionKeyFields = std::vector<uint32_t>{0},
            .rangeKeyFields = std::vector<uint32_t>{}};
    }

    // function to obtain generate key for a particular id
    static String idToKey(uint64_t keyid, uint32_t len_field){
        String key = iToString(keyid);

        K2ASSERT(log::ycsb,key.size()<=len_field,"key size exceeds field length");

        String padding( (len_field - key.size()),char(126));

        return padding + key;
    }

    YCSBData(uint64_t keyid = 0) : ID(keyid) {
        fields.resize(_num_fields());
        fields[0] = YCSBData::idToKey(keyid,_len_field()); // initialize key
    }

    YCSBData(uint64_t keyid, RandomContext& random) : ID(keyid) {

        fields.reserve(_num_fields());
        fields.push_back(YCSBData::idToKey(keyid,_len_field()));

        // populate fields with random strings of fixed length
        for(uint32_t field_no = 1; field_no < _num_fields(); ++field_no){
             fields.push_back(random.RandomString(_len_field()));
        }
    }

    uint64_t ID;
    std::vector<String> fields;
    static inline dto::Schema ycsb_schema;
    static inline dto::Schema ycsb_metadata_schema;
    static inline thread_local std::shared_ptr<dto::Schema> schema; // schema required in this format for creating SKV record
    static inline thread_local std::shared_ptr<dto::Schema> metaschema; // schema required in this format for creating SKV record
    static const inline String collectionName{"YCSB"};
    static inline std::vector<String> _fieldNames;

    static inline seastar::future<> onSetupComplete(K23SIClient& client) {
        return client.beginTxn(K2TxnOptions{})
        .then([] (auto&& txn) {
            dto::SKVRecord rec(YCSBData::collectionName, YCSBData::metaschema);  // create SKV record
            rec.serializeNext<String>(_metadata_key);
            rec.serializeNext<bool>(true);

            return seastar::do_with(
                std::move(txn),
                std::move(rec),
                [](auto& txn, auto& rec) {
                    return txn.write(rec)
                    .then([](auto&& result) mutable{
                        if (!result.status.is2xxOK()) {
                            K2LOG_E(log::ycsb, "Unable to write metadata record due to: {}", result.status);
                            return seastar::make_exception_future(std::runtime_error("Unable to write metadata record"));
                        }
                        return seastar::make_ready_future();
                    })
                    .finally([&txn] {
                        return txn.end(true)
                        .then([](auto&& response) {
                            K2ASSERT(log::ycsb, response.status==dto::K23SIStatus::OK, "failed to commit txn: {}", response);
                            return seastar::make_ready_future();
                        });
                    });
                });
        });
    }

    static inline seastar::future<> waitSchemasLoaded(K23SIClient& client) {
        return seastar::do_with(
            Deadline(20s),
            false,
            [&client](auto& deadline, auto& loaded) mutable {
                return seastar::do_until(
                    [&deadline, &loaded] {
                        return loaded || deadline.isOver();
                    },
                    [&client, &loaded]() mutable {
                        return _getSchemasLoaded(client)
                        .then([&loaded](bool result) mutable {
                            loaded = result;
                            if (!loaded) return seastar::sleep(100ms);
                            return seastar::make_ready_future();
                        });
                    })
                    .finally([&loaded] {
                        if (!loaded) {
                            return seastar::make_exception_future(std::runtime_error("Unable to load schemas within deadline"));
                        }
                        return seastar::make_ready_future();
                    });
            });
    }

    seastar::future<bool> static inline _getSchemasLoaded(K23SIClient& client) {
        K2TxnOptions opts;
        opts.priority = dto::TxnPriority::Lowest; // don't abort the write
        return client.beginTxn(std::move(opts))
        .then([](auto&& txn) {
            return seastar::do_with(
                std::move(txn),
                false,
                [](auto& txn, auto& loaded) {
                    dto::SKVRecord skv_record(YCSBData::collectionName, YCSBData::metaschema);  // create SKV record
                    skv_record.serializeNext<String>(_metadata_key);

                    return txn.read(std::move(skv_record))
                    .then([&loaded](ReadResult<dto::SKVRecord>&& result) mutable{
                        if (result.status.is2xxOK()) {
                            result.value.deserializeNext<String>();
                            auto flag = result.value.deserializeNext<bool>();
                            loaded = flag.value();
                        }
                        return seastar::make_ready_future();
                    })
                    .then([&txn, &loaded] {
                        return txn.end(true);
                    })
                    .then([&loaded] (auto&& response) mutable {
                        loaded = loaded && response.status.is2xxOK();
                        return seastar::make_ready_future<bool>(loaded);
                    });
            });
        });
    }

private:
    k2::ConfigVar<uint32_t> _len_field{"field_length"};
    k2::ConfigVar<uint32_t> _num_fields{"num_fields"};
    const static inline String _metadata_key{"ycsb_system_metadata"};
};

// function to write the given YCSB Data row
seastar::future<WriteResult> writeRow(YCSBData& row, K2TxnHandle& txn, bool erase = false, dto::ExistencePrecondition precondition = dto::ExistencePrecondition::None)
{
    dto::SKVRecord skv_record(YCSBData::collectionName, YCSBData::schema); // create SKV record

    for(auto&& field : row.fields){
        skv_record.serializeNext<String>(field); // add fields to SKV record
    }

    return txn.write<dto::SKVRecord>(skv_record, erase, precondition).then([] (WriteResult&& result) {
        if (!result.status.is2xxOK() && result.status.code!=412 && result.status.code!=404) { // k2::Statuses::S412_Precondition_Failed for write with erase=false and key already exists and k2::Statuses::S404_Not_Found for write with erase=true and key does not exist
            K2LOG_D(log::ycsb, "writeRow failed and is retryable: {}", result.status);
            return seastar::make_exception_future<WriteResult>(std::runtime_error("writeRow failed!"));
        }

        return seastar::make_ready_future<WriteResult>(std::move(result));
    });
}

// function to update the partial fields for a YCSB Data row
seastar::future<PartialUpdateResult>
partialUpdateRow(uint32_t keyid, std::vector<String> fieldValues, std::vector<uint32_t> fieldsToUpdate, K2TxnHandle& txn, uint32_t field_length) {

    dto::SKVRecord skv_record(YCSBData::collectionName, YCSBData::schema); // create SKV record

    uint32_t cur = 0;

    for(uint32_t field=0; field<YCSBData::ycsb_schema.fields.size(); field++){
        if(field==0) { // 0 is the key and cannot be updated
            skv_record.serializeNext<String>(YCSBData::idToKey(keyid,field_length)); // add key
        } else if(cur==fieldsToUpdate.size() || fieldsToUpdate[cur]!=field) { // fields to update are in order
            skv_record.serializeNull();
        } else {
            skv_record.serializeNext<String>(fieldValues[cur]); // add values of fields to be updated to SKV record
            cur++;
        }
    }

    return txn.partialUpdate<dto::SKVRecord>(skv_record, std::move(fieldsToUpdate)).then([] (PartialUpdateResult&& result) {
        if (!result.status.is2xxOK() && result.status.code!=412 && result.status.code!=404) { // 412 for precondition of exists not satisfied and 404 for key not found
            K2LOG_D(log::ycsb, "partialUpdateRow failed: {}", result.status);
            return seastar::make_exception_future<PartialUpdateResult>(std::runtime_error("partialUpdateRow failed!"));
        }

        K2LOG_D(log::ycsb, "partialUpdateRow succeeded : {}", result.status);
        return seastar::make_ready_future<PartialUpdateResult>(std::move(result));
    });
}

void SKVRecordToYCSBData(uint64_t keyid, YCSBData& row, dto::SKVRecord& skvRec){
        row.ID = keyid;
        std::optional<String> s;
        // deserialize fields
        for(uint32_t i=0; i<YCSBData::ycsb_schema.fields.size(); i++){
            s = skvRec.deserializeField<String>(i);
            if(s.has_value())
                row.fields[i] = s.value();
        }
}

void setupSchemaPointers() {
    YCSBData::schema = std::make_shared<dto::Schema>(YCSBData::ycsb_schema);
    YCSBData::metaschema = std::make_shared<dto::Schema>(YCSBData::ycsb_metadata_schema);
}
