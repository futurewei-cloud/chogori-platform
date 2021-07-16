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
#include <seastar/core/sleep.hh>

#include <k2/cmd/ycsb/data.h>
#include <k2/cmd/ycsb/Log.h>

using namespace k2;

enum operation {Read=0, Update=1, Scan=2, Insert=3};

class YCSBTxn {
public:
    virtual future<bool> run() = 0;
    virtual ~YCSBTxn() = default;
};

class YCSBBasicTxn : public YCSBTxn {
    YCSBBasicTxn(RandomContext& random, K23SIClient& client,
             RandomGenerator& requestDist, RandomGenerator& scanLengthDist, RandomGenerator& keyLengthDist) :
                        _random(random), _client(client), _requestDist(requestDist),
                        _scanLengthDist(scanLengthDist), _keyLengthDist(keyLengthDist) { };

     future<bool> run() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle&& txn) {
            _txn = std::move(txn);
            return runWithTxn();
        }).handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::ycsb, exc, "Failed to start txn");
            return make_ready_future<bool>(false);
        });
    }

private:

    future<bool> runWithTxn(){

        K2LOG_D(log::ycsb, "Starting transaction");
        return seastar::do_with((size_t)0, [this] (size_t& current_op) {
            return seastar::do_until(
                [this, &current_op] { return current_op >= _ops_per_txn(); },
                [this, &current_op] () {

                operation op = (operation)_random.BiasedInt(); // randomly pick operation based on the workload proportion

                future<> f;

                if(op==Read){
                    f = readOperation();
                }
                else if(op==Update){
                    f = updateOperation();
                }
                else if(op==Scan){
                    f = scanOperation();
                }
                else if(op==Insert){
                    f = insertOperation();
                }

                ++current_op;
                return f.discard_result();
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

    future<> readOperation(){
        K2LOG_D(log::ycsb, "Read operation started");
        int64_t keyid = _requestDist.getValue();

    }

    future<> updateOperation(){
        K2LOG_D(log::ycsb, "Update operation started");
        int64_t keyid = _requestDist.getValue();
    }

    future<> scanOperation(){
        K2LOG_D(log::ycsb, "Scan operation started");
        int64_t keyid = _requestDist.getValue();
    }

    future<> insertOperation(){
        K2LOG_D(log::ycsb, "Insert operation started");
        int64_t keyid = _requestDist.getValue();
    }

    K2TxnHandle _txn;
    RandomContext _random;
    K23SIClient _client;
    RandomGenerator _requestDist;
    RandomGenerator _scanLengthDist;
    RandomGenerator _keyLengthDist;
    ConfigVar<uint64_t> _ops_per_txn{"ops_per_txn"};
}

ConfigVar<String> _requestDistName{"request_dist"};
ConfigVar<String> _scanLengthDistName{"scan_length_dist"};
ConfigVar<String> _keyLengthDistName{"key_length_dist"};
