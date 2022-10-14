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
            K2ASSERT(log::tpcc, result.value.Balance.has_value(), "Balance is null")
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
    K2LOG_I(log::tpcc, "before w_ytd {} amount {} after w_ytd {}", _before.w_ytd, _payment._amount, _after.w_ytd);
    K2ASSERT(log::tpcc, _before.w_ytd + _payment._amount == _after.w_ytd, "Warehouse YTD did not commit!");
    K2LOG_I(log::tpcc, "before d_ytd {} amount {} after d_ytd {}", _before.d_ytd, _payment._amount, _after.d_ytd);
    K2ASSERT(log::tpcc, _before.d_ytd + _payment._amount == _after.d_ytd, "District YTD did not commit!");
    K2LOG_I(log::tpcc, "before c_ytd {} amount {} after c_ytd {}", _before.c_ytd, _payment._amount, _after.c_ytd);
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
    return do_with((DecimalD25)0, (int16_t)1,
        [this] (DecimalD25& total, int16_t& cur_d_id) {
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

                DecimalD25 w_total = *(result.value.YTD);
                double w_display = w_total.backend().extract_double();
                double total_display = total.backend().extract_double();
                K2LOG_I(log::tpcc, "YTD consistency (WARNING: displayed values may not match due to conversion of decimal type, w_total: {}, total: {}", w_display, total_display);
                K2ASSERT(log::tpcc, w_total == total, "Warehouse and district YTD totals did not match!");
                return make_ready_future<>();
            });
        });
    });
}

// Consistency condition 2: District next orderID - 1 == max OrderID == max NewOrderID
future<> ConsistencyVerify::verifyOrderIDs() {
    return _txn.read<District>(District(_cur_w_id, _cur_d_id))
    .then([this] (auto&& result) {
        if (!(result.status.is2xxOK())) {
            return make_exception_future<uint32_t>(std::runtime_error(k2::String("District should exist but does not")));
        }

        K2ASSERT(log::tpcc, result.value.NextOrderID.has_value(), "NextOrderID is null")
        return make_ready_future<uint32_t>(*(result.value.NextOrderID));
    })
    .then([this] (uint32_t nextOrderID) {
        _nextOrderID = nextOrderID;
        return _client.createQuery(tpccCollectionName, "order");
    })
    .then([this](auto&& response) mutable {
        CHECK_READ_STATUS(response);

        _query = std::move(response.query);
        _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
        _query.startScanRecord.serializeNext<int64_t>(_nextOrderID - 1);
        _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);
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
    .then([this](auto&& response) mutable {
        CHECK_READ_STATUS(response);

        _query = std::move(response.query);
        _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
        _query.startScanRecord.serializeNext<int64_t>(_nextOrderID - 1);
        _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);
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
}

// Consistency condition 3: max(new order ID) - min (new order ID) + 1 == number of new order rows
future<> ConsistencyVerify::verifyNewOrderIDs() {
    return do_with((int64_t)std::numeric_limits<int64_t>::min(),
                   (int64_t)std::numeric_limits<int64_t>::max(), (int64_t)0, [this]
                            (int64_t& max_id, int64_t& min_id, int64_t& count) {
        return _client.createQuery(tpccCollectionName, "neworder")
        .then([this, &min_id, &max_id, &count](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            _query = std::move(response.query);
            _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
            _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
            _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
            _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);

            _query.setLimit(-1);
            _query.setReverseDirection(false);
            return do_until(
            [this] () { return _query.isDone(); },
            [this, &min_id, &max_id, &count] () {
                return _txn.query(_query)
                .then([this, &min_id, &max_id, &count] (auto&& response) {
                    CHECK_READ_STATUS(response);

                    for (dto::SKVRecord& record : response.records) {
                        ++count;
                        std::optional<int64_t> id = record.deserializeField<int64_t>("OID");
                        K2ASSERT(log::tpcc, id.has_value(), "OID is null");
                        min_id = *id < min_id ? *id : min_id;
                        max_id = *id > max_id ? *id : max_id;
                    }

                    return make_ready_future<>();
                });
            });
        })
        .then([this, &min_id, &max_id, &count] () {
            if (count == 0) {
                K2LOG_I(log::tpcc, "No NewOrder records for warehouse {} district {}, skipping",
                            _cur_w_id, _cur_d_id);
                return make_ready_future<>();
            }
            K2LOG_I(log::tpcc, "max_id {} min_id {} count {}", max_id, min_id, count);
            K2ASSERT(log::tpcc, max_id - min_id + 1 == count, "There is a hole in NewOrder IDs");
            return make_ready_future<>();
        });
    });
}

// Consistency condition 4: sum of order lines from order table == number or rows in order line table
future<> ConsistencyVerify::verifyOrderLineCount() {
    return do_with((uint64_t)0, (uint64_t)0, [this] (uint64_t& olSum, uint64_t& olRows) {
        return _client.createQuery(tpccCollectionName, "order")
        .then([this, &olSum](auto&& response) {
            CHECK_READ_STATUS(response);

            _query = std::move(response.query);
            _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
            _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
            _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
            _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);

            _query.setLimit(-1);
            _query.setReverseDirection(false);

            return do_until(
            [this] () { return _query.isDone(); },
            [this, &olSum] () {
                return _txn.query(_query)
                .then([this, &olSum] (auto&& response) {
                    CHECK_READ_STATUS(response);

                    for (dto::SKVRecord& record : response.records) {
                        std::optional<int16_t> order_lines = record.deserializeField<int16_t>("OrderLineCount");
                        K2ASSERT(log::tpcc, order_lines.has_value(), "LineCount is null");
                        olSum += *order_lines;
                    }

                    return make_ready_future<>();
                });
            });
        })
        .then([this] () {
            return _client.createQuery(tpccCollectionName, "orderline");
        })
        .then([this, &olRows](auto&& response) {
            CHECK_READ_STATUS(response);

            _query = std::move(response.query);
            _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
            _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
            _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
            _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);

            _query.setLimit(-1);
            _query.setReverseDirection(false);

            return do_until(
            [this] () { return _query.isDone(); },
            [this, &olRows] () {
                return _txn.query(_query)
                .then([this, &olRows] (auto&& response) {
                    CHECK_READ_STATUS(response);

                    olRows += response.records.size();
                    return make_ready_future<>();
                });
            });
        })
        .then([this, &olSum, &olRows] () {
            K2ASSERT(log::tpcc, olSum == olRows, "Order line sum and order line row count do not match");
        });
    });
}

// Consistency condition 5: order carrier id is 0 iff there is a matching new order row
future<> ConsistencyVerify::verifyCarrierID() {
    return _client.createQuery(tpccCollectionName, "order")
    .then([this](auto&& response) {
        CHECK_READ_STATUS(response);

        _query = std::move(response.query);
        _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);

        _query.setLimit(-1);
        _query.setReverseDirection(false);

        return do_until(
        [this] () { return _query.isDone(); },
        [this] () {
            return _txn.query(_query)
            .then([this] (auto&& response) {
                CHECK_READ_STATUS(response);

                std::vector<future<>> newOrderFutures;
                for (dto::SKVRecord& record : response.records) {
                    std::optional<int32_t> carrierID = record.deserializeField<int32_t>("CarrierID");
                    std::optional<int64_t> OID = record.deserializeField<int64_t>("OID");
                    K2ASSERT(log::tpcc, carrierID.has_value(), "carrierID is null");

                    if (*carrierID == 0) {
                        future<> newOrderFut = _txn.read<NewOrder>(NewOrder(_cur_w_id, _cur_d_id, *OID))
                        .then([this] (auto&& result) {
                            CHECK_READ_STATUS(result);
                            return make_ready_future<>();
                        });
                        newOrderFutures.push_back(std::move(newOrderFut));
                    } else {
                        future<> newOrderFut = _txn.read<NewOrder>(NewOrder(_cur_w_id, _cur_d_id, *OID))
                        .then([this] (auto&& result) {
                            K2ASSERT(log::tpcc, result.status == dto::K23SIStatus::KeyNotFound,
                                        "No NewOrder row should exist with this ID");
                            return make_ready_future<>();
                        });
                        newOrderFutures.push_back(std::move(newOrderFut));
                    }
                }

                return when_all_succeed(newOrderFutures.begin(), newOrderFutures.end()).discard_result();
            });
        });
    });
}

// Helper for condition 6
future<int16_t> ConsistencyVerify::countOrderLineRows(int64_t oid) {
    return do_with((int16_t)0, Query(), [this, oid] (int16_t& count, Query& query) {
        return _client.createQuery(tpccCollectionName, "orderline")
        .then([this, &count, &query, oid](auto&& response) {
            CHECK_READ_STATUS(response);

            query = std::move(response.query);
            query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
            query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
            query.startScanRecord.serializeNext<int64_t>(oid);
            query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
            query.endScanRecord.serializeNext<int16_t>(_cur_d_id);
            query.endScanRecord.serializeNext<int64_t>(oid);

            query.setLimit(-1);
            query.setReverseDirection(false);

            return do_until(
            [&query] () { return query.isDone(); },
            [this, &count, &query, oid] () {
                return _txn.query(query)
                .then([this, &count, oid] (auto&& response) {
                    CHECK_READ_STATUS(response);

                    count += response.records.size();
                    return make_ready_future<>();
                });
            });
        })
        .then([this, &count] () {
            return make_ready_future<int16_t>(count);
        });
    });
}

// Consistency condition 6: for each order, order line count == number of rows in order line table
future<> ConsistencyVerify::verifyOrderLineByOrder() {
    return _client.createQuery(tpccCollectionName, "order")
    .then([this](auto&& response) {
        CHECK_READ_STATUS(response);

        _query = std::move(response.query);
        _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);

        _query.setLimit(-1);
        _query.setReverseDirection(false);

        return do_until(
        [this] () { return _query.isDone(); },
        [this] () {
            return _txn.query(_query)
            .then([this] (auto&& response) {
                CHECK_READ_STATUS(response);

                std::vector<future<>> orderlineFutures;
                for (dto::SKVRecord& record : response.records) {
                    std::optional<int16_t> line_count = record.deserializeField<int16_t>("OrderLineCount");
                    std::optional<int64_t> OID = record.deserializeField<int64_t>("OID");
                    K2ASSERT(log::tpcc, line_count.has_value(), "line_count is null");
                    K2ASSERT(log::tpcc, OID.has_value(), "OID is null");

                    future<> orderlineFut = countOrderLineRows(*OID)
                    .then([this, line_count, OID] (int16_t numRows) {
                        K2ASSERT(log::tpcc, *line_count == numRows,
                            "Line count of order {} does not match rows in order line table {} for OID {}",
                            *line_count, numRows, *OID);
                        return make_ready_future<>();
                    });
                    orderlineFutures.push_back(std::move(orderlineFut));
                }

                return when_all_succeed(orderlineFutures.begin(), orderlineFutures.end()).discard_result();
            });
        });
    });
}

// Consistency condition 7: order line delivery is 0 iff carrier is 0 in order
future<> ConsistencyVerify::verifyOrderLineDelivery() {
    return _client.createQuery(tpccCollectionName, "orderline")
    .then([this](auto&& response) {
        CHECK_READ_STATUS(response);

        _query = std::move(response.query);
        _query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.startScanRecord.serializeNext<int16_t>(_cur_d_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_w_id);
        _query.endScanRecord.serializeNext<int16_t>(_cur_d_id);

        _query.setLimit(-1);
        _query.setReverseDirection(false);

        return do_until(
        [this] () { return _query.isDone(); },
        [this] () {
            return _txn.query(_query)
            .then([this] (auto&& response) {
                K2LOG_D(log::tpcc, "checking response for orderline: {}", response);
                CHECK_READ_STATUS(response);

                std::vector<future<>> orderFutures;
                for (dto::SKVRecord& record : response.records) {
                    std::optional<int64_t> delivery = record.deserializeField<int64_t>("DeliveryDate");
                    std::optional<int64_t> OID = record.deserializeField<int64_t>("OID");
                    K2ASSERT(log::tpcc, OID.has_value(), "OID is null");

                    future<> orderFut = _txn.read<Order>(Order(_cur_w_id, _cur_d_id, *OID))
                    .then([this, delivery, OID] (auto&& result) {
                        CHECK_READ_STATUS(result);
                        std::optional<int32_t> carrier = result.value.CarrierID;

                        K2ASSERT(log::tpcc, *delivery != 0 ? *carrier > 0 : *carrier == 0,
                                    "deliveryData of orderline {} not consistent with order carrier {} for OID {}",
                                    *delivery, *carrier, *OID);

                        return make_ready_future<>();
                    });
                    orderFutures.push_back(std::move(orderFut));
                }

                return when_all_succeed(orderFutures.begin(), orderFutures.end()).discard_result();
            });
        });
    });
}


// Helper for consistency conditions 8 and 9
future<DecimalD25> ConsistencyVerify::historySum(bool useDistrictID) {
    return do_with((DecimalD25)0, Query(), [this, useDistrictID] (DecimalD25& sum, Query& query) {
        return _client.createQuery(tpccCollectionName, "history")
        .then([this, &sum, &query, useDistrictID](auto&& response) {
            CHECK_READ_STATUS(response);

            query = std::move(response.query);
            query.startScanRecord.serializeNext<int16_t>(_cur_w_id);
            query.endScanRecord.serializeNext<int16_t>(_cur_w_id);

            query.setLimit(-1);
            query.setReverseDirection(false);

            if (useDistrictID) {
                std::vector<dto::expression::Value> values;
                std::vector<dto::expression::Expression> exps;
                values.emplace_back(dto::expression::makeValueReference("DID"));
                int16_t d_id = _cur_d_id;
                values.emplace_back(dto::expression::makeValueLiteral<int16_t>(std::move(d_id)));
                dto::expression::Expression filter = dto::expression::makeExpression(
                                                    dto::expression::Operation::EQ,
                                                    std::move(values), std::move(exps));
                query.setFilterExpression(std::move(filter));
            }

            return do_until(
            [&query] () { return query.isDone(); },
            [this, &sum, &query] () {
                return _txn.query(query)
                .then([this, &sum] (auto&& response) {
                    CHECK_READ_STATUS(response);

                    for (dto::SKVRecord& record : response.records) {
                        std::optional<DecimalD25> amount = record.deserializeField<DecimalD25>("Amount");
                        sum += *amount;
                    }

                    return make_ready_future<>();
                });
            });
        })
        .then([this, &sum] () {
            return make_ready_future<DecimalD25>(sum);
        });
    });
}

// Consistency condition 8: Warehouse YTD == sum of history amount
future<> ConsistencyVerify::verifyWarehouseHistorySum() {
    return _txn.read<Warehouse>(Warehouse(_cur_w_id))
    .then([this] (auto&& response) {
        CHECK_READ_STATUS(response);

        std::optional<DecimalD25> ytd = response.value.YTD;
        return historySum(false)
        .then([this, ytd] (DecimalD25&& sum) {
            K2ASSERT(log::tpcc, *ytd == sum, "History sum and Warehouse YTD do not match");
        });
    });
}

// Consistency condition 9: District YTD == sum of history amount
future<> ConsistencyVerify::verifyDistrictHistorySum() {
    return _txn.read<District>(District(_cur_w_id, _cur_d_id))
    .then([this] (auto&& response) {
        CHECK_READ_STATUS(response);

        std::optional<DecimalD25> ytd = response.value.YTD;
        return historySum(true)
        .then([this, ytd] (DecimalD25&& sum) {
            K2ASSERT(log::tpcc, *ytd == sum, "History sum and District YTD do not match");
        });
    });
}

future<> ConsistencyVerify::runForEachWarehouse(consistencyOp op) {
    K2TxnOptions options{};
    options.deadline = Deadline(60s);
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
        if (fut.failed()) {
            K2LOG_W_EXC(log::tpcc, fut.get_exception(), "Txn failed");
            K2ASSERT(log::tpcc, false, "Txn failed");
        }
        EndResult result = fut.get0();
        K2ASSERT(log::tpcc, result.status.is2xxOK(), "Txn end failed, bad status: {}", result.status);
    });
}

future<> ConsistencyVerify::runForEachWarehouseDistrict(consistencyOp op) {
    K2TxnOptions options{};
    options.deadline = Deadline(120s);
    return _client.beginTxn(options)
    .then([this, op] (K2TxnHandle&& txn) {
        _txn = K2TxnHandle(std::move(txn));
        _cur_w_id = 1;
        _cur_d_id = 1;

        return repeat([this, op] () {
            return (this->*op)().then([this] () {
                _cur_d_id++;

                if (_cur_d_id > _districts_per_warehouse()) {
                    _cur_w_id++;
                    _cur_d_id = 1;
                }
                if (_cur_w_id > _max_w_id) {
                    return stop_iteration::yes;
                }

                return stop_iteration::no;
            });
        });
    }).discard_result()
    .then([this] () {
        K2LOG_I(log::tpcc, "ending transaction for runForEachWarehouseDistrict");
        return _txn.end(true);
    })
    .then_wrapped([this] (auto&& fut) {
        if (fut.failed()) {
            K2LOG_W_EXC(log::tpcc, fut.get_exception(), "Txn failed");
            K2ASSERT(log::tpcc, false, "Txn failed with exception {}", fut.get_exception());
        }
        EndResult result = fut.get0();
        K2ASSERT(log::tpcc, result.status.is2xxOK(), "Txn end failed, bad status: {}", result.status);
    });
}

future<> ConsistencyVerify::run() {
    K2LOG_I(log::tpcc, "Starting consistency verification 1");
    return runForEachWarehouse(&ConsistencyVerify::verifyWarehouseYTD)
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyWarehouseYTD consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 2: order ID");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderIDs);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyOrderIDs consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 3: neworder ID");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyNewOrderIDs);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyNewOrderIDs consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 4: order lines count");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderLineCount);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyOrderLineCount consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 5: carrier ID");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyCarrierID);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyCarrierID consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 6: order line by order");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderLineByOrder);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyOrderLineByOrder consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 7: order line delivery");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyOrderLineDelivery);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyOrderLineDelivery consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 8: warehouse ytd and history sum");
        return runForEachWarehouse(&ConsistencyVerify::verifyWarehouseHistorySum);
    })
    .then([this] () {
        K2LOG_I(log::tpcc, "verifyWarehouseHistory sum consistency success");
        K2LOG_I(log::tpcc, "Starting consistency verification 9: district ytd and history sum");
        return runForEachWarehouseDistrict(&ConsistencyVerify::verifyDistrictHistorySum);
    });
}
