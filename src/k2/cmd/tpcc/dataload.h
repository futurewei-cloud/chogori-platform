//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <vector>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/RetryStrategy.h>
#include <seastar/core/sleep.hh>

#include "mock/mock_k23si_client.h"
#include "schema.h"
#include "datagen.h"

using namespace seastar;
using namespace k2;

class DataLoader {
public:
    DataLoader() = default;
    DataLoader(TPCCData&& data) : _data(std::move(data)) {}

    future<> loadData(K23SIClient& client, int pipeline_depth)
    {
        K2TxnOptions options;
        std::vector<future<>> futures;

        for (int i=0; i < pipeline_depth; ++i) {
            futures.push_back(client.beginTxn(options).then([this] (K2TxnHandle t) {
                    return do_with(std::move(t), [this] (K2TxnHandle& txn) {
                        return insertDataLoop(txn);
                });
            }));
        }

        return when_all_succeed(futures.begin(), futures.end());
    }

private:
    future<> insertDataLoop(K2TxnHandle& txn)
    {
        return do_until(
            [this] { return _data.size() == 0; },
            [this, &txn] () {
            auto write_func = std::move(_data.back());
            _data.pop_back();

            return write_func(txn).discard_result();
        }).then([&txn] () {
            return txn.end(true);
        }).then([] (EndResult result) {
            if (!result.status.is2xxOK()) {
                return make_exception_future<>(std::runtime_error("Commit failed during bulk data load"));
            }

            return make_ready_future<>();
        });
    }

    TPCCData _data;
};

