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

#include "verify.h"

using namespace k2;
using namespace seastar;

future<> AtomicVerify::getVerificationValues(ValuesToCompare& values) {
    K2TxnOptions options{};
    options.deadline = Deadline(5s);
    return _client.beginTxn(options)
    // Capturing values by reference here, it must be an instance member variable
    .then([this, &values] (K2TxnHandle&& txn) {
        _txn = K2TxnHandle(std::move(txn));

        auto warehouse = _txn.read<Warehouse>(Warehouse(1))
        .then([this, &values] (auto&& result) {
            CHECK_READ_STATUS(result);
            K2ASSERT(log::tpcc, result.value.YTD.has_value(), "Warehouse YTD is null")
            values.w_ytd = *(result.value.YTD);
            return make_ready_future<>();
        });

        auto district = _txn.read<District>(District(1, _payment._d_id))
        .then([this, &values] (auto&& result) {
            CHECK_READ_STATUS(result);
            K2ASSERT(log::tpcc, result.value.YTD.has_value(), "District YTD is null")
            values.d_ytd = *(result.value.YTD);
            return make_ready_future<>();
        });

        auto customer = _txn.read<Customer>(Customer(_payment._c_w_id, _payment._c_d_id, _payment._c_id))
        .then([this, &values] (auto&& result) {
            CHECK_READ_STATUS(result);
            K2ASSERT(log::tpcc, result.value.Balance.has_value(), "Balacne is null")
            K2ASSERT(log::tpcc, result.value.YTDPayment.has_value(), "YTDPayment is null")
            K2ASSERT(log::tpcc, result.value.PaymentCount.has_value(), "PaymentCount is null")

            values.c_balance = *(result.value.Balance);
            values.c_ytd = *(result.value.YTDPayment);
            values.c_payments = *(result.value.PaymentCount);
            return make_ready_future<>();
        });

        return when_all_succeed(std::move(warehouse), std::move(district), std::move(customer));
    }).discard_result()
    .then([this] () {
        return _txn.end(true);
    })
    .then_wrapped([this] (auto&& fut) {
        K2ASSERT(log::tpcc, !fut.failed(), "Txn end failed");
        EndResult result = fut.get0();
        K2ASSERT(log::tpcc, result.status.is2xxOK(), "Txn end failed, bad status: {}", result.status);
    });
}

future<> AtomicVerify::runPaymentTxn(PaymentT& _payment) {
    return _payment.run().then([] (bool success) {
        K2ASSERT(log::tpcc, success, "Verficiation payment txn failed!");
    });
}

void AtomicVerify::compareCommitValues() {
    K2ASSERT(log::tpcc, _before.w_ytd + _payment._amount == _after.w_ytd, "Warehouse YTD did not commit!");
    K2ASSERT(log::tpcc, _before.d_ytd + _payment._amount == _after.d_ytd, "District YTD did not commit!");
    K2ASSERT(log::tpcc, _before.c_ytd + _payment._amount == _after.c_ytd, "Customer YTD did not commit!");
    K2ASSERT(log::tpcc, _before.c_balance - _payment._amount == _after.c_balance, "Customer Balance did not commit!");
    K2ASSERT(log::tpcc, _before.c_payments + 1 == _after.c_payments, "Customer Payment Count did not commit!");
}

void AtomicVerify::compareAbortValues() {
    K2ASSERT(log::tpcc, _before.w_ytd == _after.w_ytd, "Warehouse YTD did not abort!");
    K2ASSERT(log::tpcc, _before.d_ytd == _after.d_ytd, "District YTD did not abort!");
    K2ASSERT(log::tpcc, _before.c_ytd == _after.c_ytd, "Customer YTD did not abort!");
    K2ASSERT(log::tpcc, _before.c_balance == _after.c_balance, "Customer Balance did not abort!");
    K2ASSERT(log::tpcc, _before.c_payments == _after.c_payments, "Customer Payment Count did not abort!");
}

future<> AtomicVerify::run() {
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

// Consistency condition 1 of spec: Sum of district YTD == warehouse YTD
future<> ConsistencyVerify::verifyWarehouseYTD() {
    return do_with((std::decimal::decimal64)0, (int16_t)1,
        [this] (std::decimal::decimal64& total, int16_t& cur_d_id) {
        return do_until(
                [this, &cur_d_id] () { return cur_d_id > _districts_per_warehouse(); },
                [this, &cur_d_id, &total] () {
            K2LOG_I(log::tpcc, "Checking YTD for warehosue {} district{}", _cur_w_id, cur_d_id);
            return _txn.read<District>(District(_cur_w_id, cur_d_id))
            .then([this, &total, &cur_d_id] (auto&& result) {
                CHECK_READ_STATUS(result);
                K2ASSERT(log::tpcc, result.value.YTD.has_value(), "District YTD is null")
                total += *(result.value.YTD);

                cur_d_id++;
                return make_ready_future<>();
            });
        })
        .then([this, &total] () {
            return _txn.read<Warehouse>(Warehouse(_cur_w_id))
            .then([this, &total] (auto&& result) {
                CHECK_READ_STATUS(result);
                K2ASSERT(log::tpcc, result.value.YTD.has_value(), "Warehouse YTD is null")

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

// Consistency condition 2: District next orderID - 1 == max OrderID == max NewOrderID
future<> ConsistencyVerify::verifyOrderIDs() {
    return do_with((uint16_t)0, [this] (uint16_t& cur_d_id) {
        return do_until(
                [this, &cur_d_id] () { return cur_d_id >= _districts_per_warehouse(); },
                [this, &cur_d_id] () {
            cur_d_id++;
            return _txn.read<District>(District(_cur_w_id, cur_d_id))
            .then([this, cur_d_id] (auto&& result) {
                if (!(result.status.is2xxOK())) {
                    return make_exception_future<uint32_t>(std::runtime_error(k2::String("District should exist but does not")));
                }

                K2ASSERT(log::tpcc, result.value.NextOrderID.has_value(), "NextOrderID is null")
                return make_ready_future<uint32_t>(*(result.value.NextOrderID));
            })
            .then([this, cur_d_id] (uint32_t nextOrderID) {
                _nextOrderID = nextOrderID;
                return _client.createQuery(tpccCollectionName, "order");
            })
            .then([this, cur_d_id](auto&& response) mutable {
                CHECK_READ_STATUS(response);

                _query = std::move(response.query);
                _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
                _query.startScanRecord.serializeNext<int16_t>(cur_d_id);
                _query.startScanRecord.serializeNext<int64_t>(_nextOrderID - 1);
                _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
                _query.endScanRecord.serializeNext<int16_t>(cur_d_id);
                _query.endScanRecord.serializeNext<int64_t>(std::numeric_limits<int64_t>::max());

                _query.setLimit(-1);
                _query.setReverseDirection(false);

                return do_until(
                [this] () { return _query.isDone(); },
                [this] () {
                    return _txn.query(_query)
                    .then([this] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        K2ASSERT(log::tpcc, response.records.size() == 1 || !response.records.size(),
                                        "Should only be one records with order ID >= nextOrderID-1");
                        if (response.records.size()) {
                            dto::SKVRecord& record = response.records[0];
                            std::optional<int64_t> oid = record.deserializeField<int64_t>("OID");
                            K2ASSERT(log::tpcc, oid.has_value(), "OID is null");
                            K2ASSERT(log::tpcc, *oid == _nextOrderID - 1,
                                    "OrderID does not match nextOrderID - 1");
                        }

                        return make_ready_future<>();
                    });
                });
            })
            .then([this] () {
                return _client.createQuery(tpccCollectionName, "neworder");
            })
            .then([this, cur_d_id](auto&& response) mutable {
                CHECK_READ_STATUS(response);

                _query = std::move(response.query);
                _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
                _query.startScanRecord.serializeNext<int16_t>(cur_d_id);
                _query.startScanRecord.serializeNext<int64_t>(_nextOrderID - 1);
                _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
                _query.endScanRecord.serializeNext<int16_t>(cur_d_id);
                _query.endScanRecord.serializeNext<int64_t>(std::numeric_limits<int64_t>::max());

                _query.setLimit(-1);
                _query.setReverseDirection(false);

                return do_until(
                [this] () { return _query.isDone(); },
                [this] () {
                    return _txn.query(_query)
                    .then([this] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        K2ASSERT(log::tpcc, response.records.size() == 1 || !response.records.size(),
                                        "Should only be one records with order ID >= nextOrderID-1");
                        if (response.records.size()) {
                            dto::SKVRecord& record = response.records[0];
                            std::optional<int64_t> oid = record.deserializeField<int64_t>("OID");
                            K2ASSERT(log::tpcc, oid.has_value(), "OID is null");
                            K2ASSERT(log::tpcc, *oid == _nextOrderID - 1,
                                    "OrderID does not match nextOrderID - 1");
                        }

                        return make_ready_future<>();
                    });
                });
            });
        });
    });
}

future<> ConsistencyVerify::runForEachWarehouse(warehouseOp op) {
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
    }).discard_result()
    .then([this] () {
        return _txn.end(true);
    })
    .then_wrapped([this] (auto&& fut) {
        K2ASSERT(log::tpcc, !fut.failed(), "Txn end failed");
        EndResult result = fut.get0();
        K2ASSERT(log::tpcc, result.status.is2xxOK(), "Txn end failed, bad status: {}", result.status);
    });
}

future<> ConsistencyVerify::run() {
    K2LOG_I(log::tpcc, "Starting consistency verification");
    return runForEachWarehouse(&ConsistencyVerify::verifyWarehouseYTD)
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyWarehouseYTD consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification: order ID");
        return runForEachWarehouse(&ConsistencyVerify::verifyOrderIDs);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyOrderIDs consistency success");
    });
}

