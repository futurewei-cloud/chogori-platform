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

#include <cmath>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <seastar/core/sleep.hh>

#include "schema.h"
#include "transactions.h"
#include "Log.h"

using namespace seastar;
using namespace k2;

class AtomicVerify
{
public:
    AtomicVerify(RandomContext& random, K23SIClient& client, uint32_t max_w_id) :
            _random(random), _client(client), _payment(PaymentT(random, client, 1, max_w_id)),
            _payment2(PaymentT(random, client, 1, max_w_id)), _max_w_id(max_w_id) {}

private:
    struct ToCompare {
        std::decimal::decimal64 w_ytd;
        std::decimal::decimal64 d_ytd;
        std::decimal::decimal64 c_ytd;
        std::decimal::decimal64 c_balance;
        uint16_t c_payments;
    };

    future<> getVerificationValues(ToCompare& values) {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this, &values] (K2TxnHandle&& txn) {
            _txn = K2TxnHandle(std::move(txn));

            auto warehouse = _txn.read<Warehouse>(Warehouse(1))
            .then([this, &values] (auto&& result) {
                CHECK_READ_STATUS(result);
                values.w_ytd = *(result.value.YTD);
                return make_ready_future<>();
            });

            auto district = _txn.read<District>(District(1, _payment._d_id))
            .then([this, &values] (auto&& result) {
                CHECK_READ_STATUS(result);
                values.d_ytd = *(result.value.YTD);
                return make_ready_future<>();
            });

            auto customer = _txn.read<Customer>(Customer(_payment._c_w_id, _payment._c_d_id, _payment._c_id))
            .then([this, &values] (auto&& result) {
                CHECK_READ_STATUS(result);

                values.c_balance = *(result.value.Balance);
                values.c_ytd = *(result.value.YTDPayment);
                values.c_payments = *(result.value.PaymentCount);
                return make_ready_future<>();
            });

            return when_all_succeed(std::move(warehouse), std::move(district), std::move(customer));
        }).discard_result();
    }

    future<> runPaymentTxn(PaymentT& _payment) {
        return _payment.run().then([] (bool success) {
            K2ASSERT(log::tpcc, success, "Verficiation payment txn failed!");
        });
    }

    void compareCommitValues() {
        K2ASSERT(log::tpcc, _before.w_ytd + _payment._amount == _after.w_ytd, "Warehouse YTD did not commit!");
        K2ASSERT(log::tpcc, _before.d_ytd + _payment._amount == _after.d_ytd, "District YTD did not commit!");
        K2ASSERT(log::tpcc, _before.c_ytd + _payment._amount == _after.c_ytd, "Customer YTD did not commit!");
        K2ASSERT(log::tpcc, _before.c_balance - _payment._amount == _after.c_balance, "Customer Balance did not commit!");
        K2ASSERT(log::tpcc, _before.c_payments + 1 == _after.c_payments, "Customer Payment Count did not commit!");
    }

    void compareAbortValues() {
        K2ASSERT(log::tpcc, _before.w_ytd == _after.w_ytd, "Warehouse YTD did not abort!");
        K2ASSERT(log::tpcc, _before.d_ytd == _after.d_ytd, "District YTD did not abort!");
        K2ASSERT(log::tpcc, _before.c_ytd == _after.c_ytd, "Customer YTD did not abort!");
        K2ASSERT(log::tpcc, _before.c_balance == _after.c_balance, "Customer Balance did not abort!");
        K2ASSERT(log::tpcc, _before.c_payments == _after.c_payments, "Customer Payment Count did not abort!");
    }

    RandomContext& _random;
    K23SIClient& _client;
    PaymentT _payment;
    PaymentT _payment2;
    K2TxnHandle _txn;
    ToCompare _before;
    ToCompare _after;
    uint32_t _max_w_id;

public:
    future<> run() {
        return getVerificationValues(_before).then([this] () {
            return runPaymentTxn(_payment);
        }).then([this] () {
            return getVerificationValues(_after);
        }).then([this] () {
            compareCommitValues();
            return make_ready_future<>();
        }).then([this] () {
            _payment2._abort = true;
            return getVerificationValues(_before);
        }).then([this] () {
            return runPaymentTxn(_payment2);
        }).then([this] () {
            return getVerificationValues(_after);
        }).then([this] () {
            compareAbortValues();
            K2LOG_I(log::tpcc, "Atomicity verification success!");
            return make_ready_future<>();
        }).handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::tpcc, exc, "TPC-C Atomicity verification failed!");
            return make_ready_future<>();
        });
    }
};

class ConsistencyVerify
{
public:
    ConsistencyVerify(RandomContext& random, K23SIClient& client, uint32_t max_w_id) :
            _random(random), _client(client), _max_w_id(max_w_id) {}

private:
    using warehouseOp = future<> (ConsistencyVerify::*)();

    // Consistency condition 1 of spec
    future<> verifyWarehouseYTD() {
        return do_with((std::decimal::decimal64)0, (int16_t)1,
            [this] (std::decimal::decimal64& total, int16_t& cur_d_id) {
            return do_until(
                    [this, &cur_d_id] () { return cur_d_id > _districts_per_warehouse(); },
                    [this, &cur_d_id, &total] () {
                return _txn.read<District>(District(_cur_w_id, cur_d_id))
                .then([this, &total, &cur_d_id] (auto&& result) {
                    CHECK_READ_STATUS(result);
                    total += *(result.value.YTD);

                    cur_d_id++;
                    return make_ready_future<>();
                });
            })
            .then([this, &total] () {
                return _txn.read<Warehouse>(Warehouse(_cur_w_id))
                .then([this, &total] (auto&& result) {
                    CHECK_READ_STATUS(result);
                    std::decimal::decimal64 w_total = *(result.value.YTD);
                    double w_display = std::decimal::decimal64_to_double(w_total);
                    double total_display = std::decimal::decimal64_to_double(total);
                    K2LOG_I(log::tpcc, "YTD consistency (WARNING: displayed values may not match due to conversion of decimal type, w_total: {}, total: {}", w_display, total_display);
                    K2ASSERT(log::tpcc, w_total == total, "Warehouse and district YTD totals did not match!");
                    return make_ready_future<>();
                });
            });
        });
    }

    // Consistency condition 2
    // TODO add to this when we have scan support
    future<> verifyOrderIDs() {
        return do_with((uint16_t)0, (uint32_t)1, [this] (uint16_t& cur_d_id, uint32_t& cur_o_id) {
            return do_until(
                    [this, &cur_d_id] () { return cur_d_id >= _districts_per_warehouse(); },
                    [this, &cur_d_id, &cur_o_id] () {
                cur_d_id++;
                return _txn.read<District>(District(_cur_w_id, cur_d_id))
                .then([this, &cur_d_id, &cur_o_id] (auto&& result) {
                    if (!(result.status.is2xxOK())) {
                        return make_exception_future<uint32_t>(std::runtime_error(k2::String("District should exist but does not")));
                    }

                    return make_ready_future<uint32_t>(*(result.value.NextOrderID));
                })
                .then([this, &cur_d_id, &cur_o_id] (uint32_t nextOrderID) {
                    cur_o_id = 1;

                    return do_until(
                            [&cur_o_id, nextOrderID] () { return cur_o_id > nextOrderID + 5; },
                            [this, &cur_o_id, &cur_d_id, nextOrderID] () {
                        return _txn.read<Order>(Order(_cur_w_id, cur_d_id, cur_o_id))
                        .then([&cur_o_id, nextOrderID] (auto&& result) {
                            if (cur_o_id < nextOrderID) {
                                CHECK_READ_STATUS(result);
                            } else {
                                K2ASSERT(log::tpcc, result.status == dto::K23SIStatus::KeyNotFound, "OrderID exists higher than NextOrderID");
                            }

                            cur_o_id++;
                            return make_ready_future<>();
                        });
                    });
                });
            });
        });
    }

    future<> runForEachWarehouse(warehouseOp op) {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this, op] (K2TxnHandle&& txn) {
            _txn = K2TxnHandle(std::move(txn));
            _cur_w_id = 1;

            return repeat([this, op] () {
                return (this->*op)().then([this] () {
                    _cur_w_id++;
                    if (_cur_w_id > _max_w_id) {
                        return stop_iteration::yes;
                    }

                    return stop_iteration::no;
                });
            });
        }).discard_result();
    }

    RandomContext& _random;
    K23SIClient& _client;
    K2TxnHandle _txn;
    uint32_t _max_w_id;
    uint32_t _cur_w_id;
    ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};

public:
    future<> run() {
        K2LOG_I(log::tpcc, "Starting consistency verification");
        return runForEachWarehouse(&ConsistencyVerify::verifyWarehouseYTD)
        .then([this] () {
            K2LOG_I(log::tpcc, "Starting consistency verification: order ID");
            return runForEachWarehouse(&ConsistencyVerify::verifyOrderIDs);
        });
    }
};

