//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <vector>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>
#include <seastar/core/sleep.hh>

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
        K2TxnOptions options{};
        options.deadline = Deadline(6000s);
        std::vector<future<>> futures;

        for (int i=0; i < pipeline_depth; ++i) {
            futures.push_back(client.beginTxn(options).then([this] (K2TxnHandle&& t) {
                    K2DEBUG("txn begin in load data");
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
            K2DEBUG("remaining data size=" << _data.size());
            auto write_func = std::move(_data.back());
            _data.pop_back();

            return write_func(txn).discard_result();
        }).then([&txn] () {
            return txn.end(true);
        }).then([] (EndResult&& result) {
            if (!result.status.is2xxOK()) {
                K2INFO("Failed to commit: " << result.status);
                return make_exception_future<>(std::runtime_error("Commit failed during bulk data load"));
            }

            return make_ready_future<>();
        });
    }

    TPCCData _data;
};

