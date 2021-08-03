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

#include <vector>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>
#include <seastar/core/sleep.hh>

#include <k2/cmd/ycsb/data.h>
#include <k2/cmd/ycsb/Log.h>

using namespace k2;

class DataLoader {
public:
    DataLoader() = default;

    seastar::future<> loadData(K23SIClient& client, int pipeline_depth, size_t startIdxShard, size_t endIdxShard)
    {
        K2TxnOptions options{};
        options.deadline = Deadline(ConfigDuration("dataload_txn_timeout", 600s)());
        options.syncFinalize = true;

        double propSkip = (double)_num_inserts()/(_num_records()+_num_inserts()); // proportion of records to Skip while loading
        RandomContext random_context(0,{propSkip, 1-propSkip});

        K2LOG_D(log::ycsb, "pipeline depth and data load per txn ={}, {}", pipeline_depth, _writes_per_load_txn());
        return seastar::do_with((size_t)startIdxShard, [this, options, pipeline_depth, &client, random_context, startIdxShard, endIdxShard] (size_t& start_idx){
            return seastar::do_until(
                [this, &start_idx, endIdxShard] { return start_idx>=endIdxShard;  },
                [this, options, pipeline_depth, &client, &start_idx, random_context, endIdxShard] {
                    std::vector<seastar::future<>> futures;

                    for (int i=0; i < pipeline_depth; ++i) {
                        futures.push_back(client.beginTxn(options).then([this, i, start_idx, random_context, endIdxShard] (K2TxnHandle&& t) {
                                K2LOG_D(log::ycsb, "txn begin in load data");
                                return seastar::do_with(std::move(t), [this, i, start_idx, random_context, endIdxShard] (K2TxnHandle& txn) {
                                    size_t idx = start_idx + i*_writes_per_load_txn();
                                    return insertDataLoop(txn, idx, random_context, endIdxShard);
                            });
                        }));
                    }
                    start_idx = start_idx + pipeline_depth*_writes_per_load_txn();
                    return seastar::when_all_succeed(futures.begin(), futures.end());
            });
         });
    }

private:
    seastar::future<> insertDataLoop(K2TxnHandle& txn, size_t start_idx, RandomContext random_context, size_t endIdxShard)
    {
        K2LOG_D(log::ycsb, "Starting transaction, start_idx is {}", start_idx);
        return seastar::do_with((size_t)0, std::move(random_context), [this, &txn, start_idx, endIdxShard] (size_t& current_size, RandomContext& random_c) {
            return seastar::do_until(
                [this, &current_size, start_idx, endIdxShard] { return ((current_size >= _writes_per_load_txn()) || ((start_idx + current_size)>= endIdxShard)); },
                [this, &current_size, &txn, start_idx, &random_c] () {
                uint8_t isLoad = random_c.BiasedInt();
                if(isLoad || (_requestDistName()=="latest" && (start_idx + current_size)<_num_records())){ // load record with prob = _num_records / _num_keys or if latest load all records uptil num_records()
                    K2LOG_D(log::ycsb, "Record being loaded now in this txn is {}", start_idx + current_size);

                    YCSBData row(start_idx + current_size, random_c); // generate row
                    ++current_size;

                    return writeRow(row, txn).discard_result();
                }
                K2LOG_D(log::ycsb, "Record {} skipped", start_idx + current_size);
                ++current_size;

                return seastar::make_ready_future<>();

            }).then([&txn] () {
                K2LOG_D(log::ycsb, "Ending transaction");
                return txn.end(true);
            }).then([] (EndResult&& result) {
                if (!result.status.is2xxOK()) {
                    K2LOG_E(log::ycsb, "Failed to commit: {}", result.status);
                    return seastar::make_exception_future<>(std::runtime_error("Commit failed during bulk data load"));
                }

                return seastar::make_ready_future<>();
            });
        });
    }

    ConfigVar<size_t> _writes_per_load_txn{"writes_per_load_txn"};
    ConfigVar<size_t> _num_records{"num_records"};
    ConfigVar<size_t> _num_inserts{"num_records_insert"};
    ConfigVar<String> _requestDistName{"request_dist"};
};

