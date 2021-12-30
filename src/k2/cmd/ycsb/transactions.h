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

#include <utility>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>

#include <k2/cmd/ycsb/data.h>
#include <k2/cmd/ycsb/Log.h>
#include <k2/cmd/ycsb/ycsb_rand.h>

using namespace k2;
// a simple retry strategy, stop after a number of retries
class FixedRetryStrategy {
public:
    FixedRetryStrategy(int retries) : _retries(retries), _try(0), _success(false) {
    }

    ~FixedRetryStrategy() = default;

template<typename Func>
    seastar::future<> run(Func&& func) {
        K2LOG_D(log::ycsb, "First attempt");
        return seastar::do_until(
            [this] { return this->_success || this->_try >= this->_retries; },
            [this, func=std::move(func)] () mutable {
                this->_try++;
                return func().
                    then_wrapped([this] (auto&& fut) {
                        _success = !fut.failed() && fut.get0();
                        K2LOG_D(log::ycsb, "round {} ended with success={}", _try, _success);
                        return seastar::make_ready_future<>();
                    });
            }).then_wrapped([this] (auto&& fut) {
                if (fut.failed() || !_success) {
                    K2LOG_W(log::ycsb, "Run failed");
                    return seastar::make_exception_future<>(std::runtime_error("Run failed:"));
                }
                return seastar::make_ready_future<>();
            });
    }

private:
    // how many times we should retry
    int _retries;
    // which try we're on
    int _try;
    // indicate if the latest round has succeeded (so that we can break the retry loop)
    bool _success;
};

class YCSBTxn {

public:
    seastar::future<bool> run() {
        // retry 10 times for a failed transaction
        return seastar::do_with(FixedRetryStrategy(10),  [this] (auto& retryStrategy) {
            return retryStrategy.run([this]() {
                return attempt();
            }).then_wrapped([this] (auto&& fut) {
                return seastar::make_ready_future<bool>(!fut.failed());
            });
        });
    }

    YCSBTxn(RandomContext& random, K23SIClient& client,
             std::shared_ptr<RandomGenerator> requestDist, std::shared_ptr<RandomGenerator> scanLengthDist) :
                        _random(random), _client(client), _failed(false), _requestDist(requestDist), _scanLengthDist(scanLengthDist) {}

     seastar::future<bool> attempt() {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle&& txn) {
            _txn = std::move(txn);
            return runWithTxn();
        }).handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::ycsb, exc, "Failed to start txn");
            return seastar::make_ready_future<bool>(false);
        });
    }

    // function to get all operations in transcation
    std::vector<Operation>& getOps(){
        return _ops;
    }

    // function to get number of inserts missed
    uint64_t getInsertMisses(){
        return _insertMissesLatest;
    }

private:

    seastar::future<bool> runWithTxn(){
        K2LOG_D(log::ycsb, "Starting transaction");
        _txnkeyids.reserve(_ops_per_txn());
        return seastar::do_with((size_t)0, [this] (size_t& current_op) {
            return seastar::do_until(
                [this, &current_op] { return current_op >= _ops_per_txn(); },
                [this, &current_op] () {

                if(_txnkeyids.size()<=current_op){
                    _txnkeyids.push_back(_requestDist->getValue()); // randomly pick key for current operation
                    _ops.push_back((Operation)_random.BiasedInt()); // randomly pick operation based on the workload proportion
                }

                _keyid = _txnkeyids[current_op]; // use same key values if we are retrying txn
                ++current_op;
                switch(_ops[current_op-1]){
                    case Operation::Read: {
                        return readOperation();
                    }
                    case Operation::Update: {
                        return updateOperation();
                    }
                    case Operation::Scan: {
                        return scanOperation();
                    }
                    case Operation::Insert: {
                        return insertOperation();
                    }
                    case Operation::Delete: {
                        return deleteOperation();
                    }
                    default: {
                        K2ASSERT(log::ycsb, false, "Invalid operation");
                    }
                };
            })
            // commit txn
            .then_wrapped([this] (auto&& fut) {
                if (fut.failed()) {
                    _failed = true;
                    fut.ignore_ready_future();
                    return _txn.end(false);
                }

                fut.ignore_ready_future();
                K2LOG_D(log::ycsb, "Txn finished");

                return _txn.end(true);
            }).then_wrapped([this] (auto&& fut) {
                if (fut.failed()) {
                    _failed = true;
                    fut.ignore_ready_future();
                    return seastar::make_ready_future<bool>(false);
                }

                EndResult result = fut.get0();
                if (result.status.is2xxOK() && !_failed) {

                    return seastar::make_ready_future<bool>(true);
                }

                return seastar::make_ready_future<bool>(false);
            });
        });
    }

    seastar::future<> readOperation(){
        K2LOG_D(log::ycsb, "Read operation started for keyid {}",_keyid);
        String key = YCSBData::idToKey(_keyid,_field_length());
        dto::SKVRecord skv_record(YCSBData::collectionName, YCSBData::schema); // create SKV record

        skv_record.serializeNext<String>(key);

        return _txn.read(std::move(skv_record)).then([this] (auto&& result) {
            CHECK_READ_STATUS(result);
            K2LOG_D(log::ycsb, "Read succeeded : {}", result.status);
            // store returned read result in _data
            SKVRecordToYCSBData(_keyid,_data,result.value);
            return seastar::make_ready_future();
        });
    }

    seastar::future<> updateOperation(){
        K2LOG_D(log::ycsb, "Update operation started for keyid {}", _keyid);

        // select number of fields to update uniformly at random and also fill their values at random
        int32_t numFields = _random.UniformRandom(1,_max_fields_update());
        std::vector<String> fieldValues(numFields,"");
        std::vector<uint32_t> fieldsToUpdate(numFields,0);

        std::set<uint32_t> fields = _random.RandomSetInt(numFields, 1, _num_fields()-1); // select the fields to be updated, do not consider 0 because it is key

        uint32_t cur = 0;
        for(auto&& field: fields){
            fieldsToUpdate[cur] = field;
            fieldValues[cur] = _random.RandomString(_field_length());
            cur++;
        }

        return partialUpdateRow(_keyid,std::move(fieldValues),std::move(fieldsToUpdate),_txn,_field_length()).discard_result();
    }

    seastar::future<> scanOperation(){
        K2LOG_D(log::ycsb, "Scan operation started");

        return _client.createQuery(YCSBData::collectionName, "ycsb_data")
        .then([this](auto&& response) mutable {
            CHECK_READ_STATUS(response);
            // make Query request and set query rules
            _query_scan = std::move(response.query);
            _query_scan.startScanRecord.serializeNext<String>(YCSBData::idToKey(_keyid,_field_length()));
            _query_scan.setLimit(_scanLengthDist->getValue()); // get specified number of entries (got from scanLength Distribution) starting from given key
            _query_scan.setReverseDirection(false);

            std::vector<String> projection(YCSBData::_fieldNames); // make projection on all fields
            _query_scan.addProjection(projection);
            dto::expression::Expression filter{};   // make filter Expression
            _query_scan.setFilterExpression(std::move(filter));
            _scanResult.clear();

            return seastar::do_with((bool)false,  [this] (bool& done) {
                return seastar::do_until(
                [this, &done] () { return done; },
                [this, &done] () {
                    return _txn.query(_query_scan)
                    .then([this, &done] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        done = response.status.is2xxOK() ? _query_scan.isDone() : true;

                        _scanResult.reserve(_scanResult.size()+response.records.size()); // reserve space for writing the records in scan result
                        for (dto::SKVRecord& rec : response.records) {
                            YCSBData data;
                            SKVRecordToYCSBData(0,data,rec); // convert skvrecord to YCSBData object
                            _scanResult.push_back(data); // store result in _scanResult
                        }

                        return seastar::make_ready_future();
                    });
                })
                .then([this] () {
                    K2LOG_D(log::ycsb, "Scan succeeded");
                    return seastar::make_ready_future();
                });
            });
        });
    }

    seastar::future<> insertOperation(){

        if(_requestDistName()!="latest") {
            K2LOG_D(log::ycsb, "Insert operation started for keyid {}", _keyid);
            YCSBData row(_keyid, _random); // generate row
            return writeRow(row, _txn).discard_result();
        }

        _keyid = _requestDist->getMaxValue()+1;
        YCSBData row(_keyid,_random); // key is max known (latest) key + 1

        //handle latest distribution separately to identify insert fails and increment latest known record max key value
        K2LOG_D(log::ycsb, "Insert operation started for keyid {}", _keyid);
        return writeRow(row, _txn, false, dto::ExistencePrecondition::NotExists) // if record with given key already exists must return error so we set precondition to Exists
            .then_wrapped([this] (auto&& fut) {
                if (fut.failed()) {
                    fut.ignore_ready_future();
                    return seastar::make_exception_future<>(std::runtime_error("Insert failed!"));
                }

                WriteResult result = fut.get0();
                if (result.status.is2xxOK() || result.status.code == 412) { // key inserted or already exists
                    if(_requestDist->getMaxValue() < _keyid) { // update bounds of distribution
                        _requestDist->updateBounds(0,_keyid);
                    }
                    if(result.status.code == 412) { // insert missed because record exists
                        _insertMissesLatest++; // increment misses counter
                    }
                }

                return seastar::make_ready_future();
            });
    }

    seastar::future<> deleteOperation(){
        K2LOG_D(log::ycsb, "Delete operation started for keyid {}", _keyid);
        YCSBData row(_keyid); // generate row
        return writeRow(row, _txn, true).discard_result();
    }

    Query _query_scan;
    K2TxnHandle _txn;
    RandomContext& _random;
    K23SIClient& _client;
    bool _failed;
    YCSBData _data; // row read from read operation
    uint64_t _keyid; // keyid for current op
    std::vector<uint64_t> _txnkeyids; // keyids used in this txn stored for retries
    std::vector<Operation> _ops; // ops used in this txn stored for retries
    std::vector<YCSBData> _scanResult; // rows read from scan operation
    std::shared_ptr<RandomGenerator> _requestDist; // Request distribution for selecting keys
    std::shared_ptr<RandomGenerator> _scanLengthDist; // Request distribution for selecting length of scan
    uint64_t _insertMissesLatest{0}; // number of inserts missed when request distribution is Latest distribution

private:
    ConfigVar<uint64_t> _ops_per_txn{"ops_per_txn"};
    ConfigVar<uint32_t> _field_length{"field_length"};
    ConfigVar<uint32_t> _num_fields{"num_fields"};
    ConfigVar<uint32_t> _max_fields_update{"max_fields_update"};
    ConfigVar<String> _requestDistName{"request_dist"};
};
