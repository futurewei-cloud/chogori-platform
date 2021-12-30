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

#pragma once

#include <vector>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>

#include "schema.h"
#include "datagen.h"
#include "Log.h"

using namespace seastar;
using namespace k2;

class DataLoader {
public:
    DataLoader() = default;
    DataLoader(TPCCData&& data) : _data(std::move(data)) {}

    future<> loadData(K23SIClient& client, int pipeline_depth)
    {
        K2TxnOptions options{};
        options.deadline = Deadline(ConfigDuration("dataload_txn_timeout", 600s)());
        options.syncFinalize = true;
        std::vector<future<>> futures;

        return do_until(
            [this] { return _data.size() == 0; },
            [this, options, pipeline_depth, &client] {
                std::vector<future<>> futures;

                for (int i=0; i < pipeline_depth; ++i) {
                    futures.push_back(client.beginTxn(options).then([this] (K2TxnHandle&& t) {
                            K2LOG_D(log::tpcc, "txn begin in load data");
                            return do_with(std::move(t), [this] (K2TxnHandle& txn) {
                                return insertDataLoop(txn);
                        });
                    }));
                }

                return when_all_succeed(futures.begin(), futures.end());
        });
    }

private:
    future<> insertDataLoop(K2TxnHandle& txn)
    {
        return do_with((size_t)0, [this, &txn] (size_t& current_size) {
            return do_until(
                [this, &current_size] { return _data.size() == 0 || current_size >= _writes_per_load_txn(); },
                [this, &current_size, &txn] () {
                K2LOG_D(log::tpcc, "remaining data size={}", _data.size());
                auto write_func = std::move(_data.back());
                _data.pop_back();
                ++current_size;

                return write_func(txn).discard_result();
            }).then([&txn] () {
                return txn.end(true);
            }).then([] (EndResult&& result) {
                if (!result.status.is2xxOK()) {
                    K2LOG_E(log::tpcc, "Failed to commit: {}", result.status);
                    return make_exception_future<>(std::runtime_error("Commit failed during bulk data load"));
                }

                return make_ready_future<>();
            });
        });
    }

    TPCCData _data;
    ConfigVar<size_t> _writes_per_load_txn{"writes_per_load_txn"};
};

