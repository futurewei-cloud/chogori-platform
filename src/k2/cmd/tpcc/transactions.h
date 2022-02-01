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

#include <utility>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/RetryStrategy.h>

#include "schema.h"
#include "Log.h"

using namespace seastar;
using namespace k2;

// a simple retry strategy, stop after a number of retries
class FixedRetryStrategy {
public:
    FixedRetryStrategy(int retries) : _retries(retries), _try(0), _success(false) {
    }

    ~FixedRetryStrategy() = default;

template<typename Func>
    future<> run(Func&& func) {
        K2LOG_D(log::tpcc, "First attempt");
        return do_until(
            [this] { return this->_success || this->_try >= this->_retries; },
            [this, func=std::move(func)] () mutable {
                this->_try++;
                return func().
                    then_wrapped([this] (auto&& fut) {
                        _success = !fut.failed() && fut.get0();
                        K2LOG_D(log::tpcc, "round {} ended with success={}", _try, _success);
                        return make_ready_future<>();
                    });
            }).then_wrapped([this] (auto&& fut) {
                if (!fut.failed()) {
                    fut.ignore_ready_future();
                }

                if (fut.failed()) {
                    K2LOG_W_EXC(log::tpcc, fut.get_exception(), "Txn failed");
                } else if (!_success) {
                    K2LOG_D(log::tpcc, "Txn attempt failed");
                    return make_exception_future<>(std::runtime_error("Attempt failed"));
                }
                return make_ready_future<>();
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

class AtomicVerify;

class TPCCTxn {
public:
    virtual future<bool> attempt() = 0;
    virtual ~TPCCTxn() = default;

    future<bool> run() {
        // retry 10 times for a failed transaction
        return do_with(FixedRetryStrategy(10),  [this] (auto& retryStrategy) {
            return retryStrategy.run([this]() {
                return attempt();
            }).then_wrapped([this] (auto&& fut) {
                bool succeed = !fut.failed();
                if (succeed)
                    fut.ignore_ready_future();
                else
                    K2LOG_E(log::tpcc, "TPCC Txn failed: {}", fut.get_exception());
                return make_ready_future<bool>(succeed);
            });
        })
        .handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::tpcc, exc, "Txn failed after retries");
            return make_ready_future<bool>(false);
        });
    }
};

class PaymentT : public TPCCTxn
{
public:
    PaymentT(RandomContext& random, K23SIClient& client, int16_t w_id, int16_t max_w_id) :
                        _random(random), _client(client), _w_id(w_id) {

        _d_id = random.UniformRandom(1, _districts_per_warehouse());
        // customer id will be re-assign based on probability later during customerUpdate()
        _c_id = random.NonUniformRandom(1023, 1, _customers_per_district());
        uint32_t local = random.UniformRandom(1, 100);
        if (local <= 85 || max_w_id == 1) {
            _c_w_id = _w_id;
            _c_d_id = _d_id;
        } else {
            do {
                _c_w_id = random.UniformRandom(1, max_w_id);
            } while (_c_w_id == _w_id);
            _c_d_id = random.UniformRandom(1, _districts_per_warehouse());
        }

        _amount = random.UniformRandom(100, 500000);
        _amount /= 100;

        _failed = false;
        _abort = false;
        _force_original_cid = false;
    }

    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle&& txn) {
            _txn = std::move(txn);
            return runWithTxn();
        }).handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::tpcc, exc, "Failed to start txn");
            return make_ready_future<bool>(false);
        });
    }

private:
    future<bool> runWithTxn() {
        future<> warehouse_update = warehouseUpdate();
        future<> district_update = districtUpdate();
        future<> CId_get = getCIdByLastNameViaIndex();

        return when_all_succeed(std::move(warehouse_update), std::move(district_update), std::move(CId_get)).discard_result()
        .then([this] () {
            future<> history_update = historyUpdate();
            future<> customer_update = customerUpdate();

            return when_all_succeed(std::move(customer_update), std::move(history_update)).discard_result()
            .then_wrapped([this] (auto&& fut) {
                if (fut.failed()) {
                    _failed = true;
                    K2LOG_W_EXC(log::tpcc, fut.get_exception(), "Payment Txn failed");
                    return _txn.end(false);
                }

                fut.ignore_ready_future();
                K2LOG_D(log::tpcc, "Payment txn finished");

                return _txn.end(!_abort);
            }).then_wrapped([this] (auto&& fut) {
                if (fut.failed()) {
                    _failed = true;
                    K2LOG_W_EXC(log::tpcc, fut.get_exception(), "Payment Txn failed");
                    return make_ready_future<bool>(false);
                }

                EndResult result = fut.get0();

                if (result.status.is2xxOK() && ! _failed) {
                    return make_ready_future<bool>(true);
                }

                return make_ready_future<bool>(false);
            });
        });
    }

    future<> warehouseUpdate() {
        return _txn.read<Warehouse>(Warehouse(_w_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);
            _w_name = *(result.value.Name);
            Warehouse warehouse(_w_id);
            warehouse.YTD = *(result.value.YTD) + _amount;
            return partialUpdateRow<Warehouse, std::vector<uint32_t>>(warehouse, {2}, _txn).discard_result();
        });
    }

    future<> districtUpdate() {
        return _txn.read<District>(District(_w_id, _d_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);
            _d_name = *(result.value.Name);
            District district(_w_id, _d_id);
            district.YTD = *(result.value.YTD) + _amount;
            return partialUpdateRow<District, std::vector<uint32_t>>(district, {3}, _txn).discard_result();
        });
    }

    future<> customerUpdate() {
        return _txn.read<Customer>(Customer(_c_w_id, _c_d_id, _c_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);

            Customer customer(_c_w_id, _c_d_id, _c_id);
            customer.Balance = *(result.value.Balance) - _amount;
            customer.YTDPayment = *(result.value.YTDPayment) + _amount;
            customer.PaymentCount = *(result.value.PaymentCount) + 1;
            k2::String str500(k2::String::initialized_later{}, 500);
            customer.Info = str500;

            if (*(result.value.Credit) == "BC") {
                size_t shift_size = sizeof(_c_id) + sizeof(_c_d_id) + sizeof(_d_id) + sizeof(_w_id) + sizeof(_amount);
                memmove((char*)customer.Info->c_str() + shift_size, (char*)result.value.Info->c_str(), 500-shift_size);
                uint32_t offset = 0;
                memcpy((char*)customer.Info->c_str() + offset, &_c_id, sizeof(_c_id));
                offset += sizeof(_c_id);
                memcpy((char*)customer.Info->c_str() + offset, &_c_d_id, sizeof(_c_d_id));
                offset += sizeof(_c_d_id);
                memcpy((char*)customer.Info->c_str() + offset, &_d_id, sizeof(_d_id));
                offset += sizeof(_d_id);
                memcpy((char*)customer.Info->c_str() + offset, &_w_id, sizeof(_w_id));
                offset += sizeof(_w_id);
                memcpy((char*)customer.Info->c_str() + offset, &_amount, sizeof(_amount));
            }

            return partialUpdateRow<Customer, std::vector<k2::String>>(customer, {"Balance", "YTDPayment", "PaymentCount", "Info"}, _txn).discard_result();
        });
    }

    future<> historyUpdate() {
        History history(_w_id, _d_id, _c_id, _c_w_id, _c_d_id, _amount, _w_name.c_str(), _d_name.c_str());
        return writeRow<History>(history, _txn).discard_result();
    }

    // get customer ID by last name
    // 60% select customer by last name; 40%, the _c_id has already been set randomly at the constructor
    future<> getCIdByLastNameViaIndex() {
        uint32_t cid_type = _random.UniformRandom(1, 100);
        if (cid_type <= 60 || _force_original_cid) {
            return make_ready_future();
        }


        return _client.createQuery(tpccCollectionName, "idx_customer_name")
        .then([this](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            k2::String lastName = _random.RandowLastNameString();
            // make Query request and set query rules.
            // 1. range:[ (_w_id, _d_id, c_last), (_w_id, _d_id, c_last) ]; 2. no record number limits; 3. forward direction scan;
            // 4. projection "c_id" field for ascending sorting;
            _query = std::move(response.query);
            _query.startScanRecord.serializeNext<int16_t>(_c_w_id);
            _query.startScanRecord.serializeNext<int16_t>(_c_d_id);
            _query.startScanRecord.serializeNext<k2::String>(lastName);
            _query.endScanRecord.serializeNext<int16_t>(_c_w_id);
            _query.endScanRecord.serializeNext<int16_t>(_c_d_id);
            _query.endScanRecord.serializeNext<k2::String>(lastName);

            _query.setLimit(-1);
            _query.setReverseDirection(false);
            std::vector<String> projection{"CID"}; // make projection
            _query.addProjection(projection);

            return do_with(std::vector<std::vector<dto::SKVRecord>>(), false, (uint32_t)0,
            [this] (std::vector<std::vector<dto::SKVRecord>>& result_set, bool& done, uint32_t& count) {
                return do_until(
                [this, &done] () { return done; },
                [this, &result_set, &done, &count] () {
                    return _txn.query(_query)
                    .then([this, &result_set, &done, &count] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        done = response.status.is2xxOK() ? _query.isDone() : true;
                        count += response.records.size();
                        result_set.push_back(std::move(response.records));

                        return make_ready_future();
                    });
                })
                .then([this, &result_set, &count] () {
                    std::vector<int32_t> cIdSort;
                    for (std::vector<dto::SKVRecord>& set : result_set) {
                        for (dto::SKVRecord& rec : set) {
                            std::optional<int32_t> cIdOpt = rec.deserializeField<int32_t>("CID");
                            cIdSort.push_back(cIdOpt.value());
                        }
                    }

                    // retrieve at the position n/2 rounded up in the sorted set from idx_customer_name table.
                    if (cIdSort.size()) {
                        std::sort(cIdSort.begin(), cIdSort.end());
                        _c_id = cIdSort[cIdSort.size() / 2];
                    }

                    return make_ready_future();
                });
            });
        });
    }

    RandomContext& _random;
    K23SIClient& _client;
    K2TxnHandle _txn;
    Query _query;
    int16_t _w_id;
    int16_t _c_w_id;
    int32_t _c_id;
    std::decimal::decimal64 _amount;
    int16_t _d_id;
    int16_t _c_d_id;
    k2::String _w_name;
    k2::String _d_name;
    bool _failed;
    bool _abort; // Used by verification test to force an abort
    bool _force_original_cid; // Used by verification test to force the cid to not change

private:
    ConfigVar<int16_t> _districts_per_warehouse{"districts_per_warehouse"};
    ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};

friend class AtomicVerify;
};

class NewOrderT : public TPCCTxn
{
public:
    NewOrderT(RandomContext& random, K23SIClient& client, int16_t w_id, int16_t max_w_id) :
                        _random(random), _client(client), _w_id(w_id), _max_w_id(max_w_id),
                        _failed(false), _order(random, w_id) {}

    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle&& txn) {
            _txn = std::move(txn);
            return runWithTxn();
        }).handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::tpcc, exc, "Failed to start txn");
            return make_ready_future<bool>(false);
        });
    }

private:
    future<bool> runWithTxn() {
        // Get warehouse row, only used for tax rate in total amount calculation
        future<> warehouse_f = _txn.read<Warehouse>(Warehouse(_w_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);
            _w_tax = *(result.value.Tax);
            return make_ready_future();
        });

        // Get customer row, only used for discount rate in total amount calculation
        future<> customer_f = _txn.read<Customer>(Customer(_w_id, *(_order.DistrictID), *(_order.CustomerID)))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);
            _c_discount = *(result.value.Discount);
            return make_ready_future();
        });

         future<> main_f = _txn.read<District>(District(_w_id, *(_order.DistrictID)))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);

            // Get and write NextOrderID in district row
            _order.OrderID = *(result.value.NextOrderID);
            _d_tax = *(result.value.Tax);
            District district(*result.value.WarehouseID, *result.value.DistrictID);
            district.NextOrderID = (*result.value.NextOrderID) + 1;
            future<PartialUpdateResult> district_update = partialUpdateRow<District, std::vector<k2::String>>(district, {"NextOID"}, _txn);

            // Write NewOrder row
            NewOrder new_order(_order);
            future<WriteResult> new_order_update = writeRow<NewOrder>(new_order, _txn);

            makeOrderLines();

            // Write Order row
            future<WriteResult> order_update = writeRow<Order>(_order, _txn);

            IdxOrderCustomer idx_order_customer(_order.WarehouseID.value(), _order.DistrictID.value(),
                                     _order.CustomerID.value(), _order.OrderID.value());

            // Write order secondary index row
            future<WriteResult> order_idx_update = writeRow<IdxOrderCustomer>(idx_order_customer, _txn);

            future<> line_updates = parallel_for_each(_lines.begin(), _lines.end(), [this] (OrderLine& line) {
                return _txn.read<Item>(Item(*(line.ItemID)))
                .then([this, i_id=*(line.ItemID)] (auto&& result) {
                    if (result.status == dto::K23SIStatus::KeyNotFound) {
                        return make_exception_future<Item>(std::runtime_error("Bad ItemID"));
                    } else if (!result.status.is2xxOK()) {
                        K2LOG_D(log::tpcc, "Bad read status: {}", result.status);
                        return make_exception_future<Item>(std::runtime_error("Bad read status"));
                    }

                    return make_ready_future<Item>(std::move(result.value));

                }).then([this, supply_id=*(line.SupplyWarehouseID)] (Item&& item) {
                    return _txn.read<Stock>(Stock(supply_id, *(item.ItemID)))
                    .then([item, supply_id] (auto&& result) {
                        if (!result.status.is2xxOK()) {
                            K2LOG_D(log::tpcc, "Bad read status: {}", result.status);
                            return make_exception_future<std::pair<Item, Stock>>(std::runtime_error("Bad read status"));
                        }
                        return make_ready_future<std::pair<Item, Stock>>(std::make_pair(std::move(item), result.value));
                    });

                }).then([this, line] (std::pair<Item, Stock>&& pair) mutable {
                    auto& [item, stock] = pair;
                    *(line.Amount) = *(item.Price) * *(line.Quantity);
                    _total_amount += *(line.Amount);
                    line.DistInfo = stock.getDistInfo(*(line.DistrictID));

                    std::vector<k2::String> stockUpdateFields;
                    Stock updateStock(*stock.WarehouseID, *stock.ItemID);
                    updateStockRow(stock, line, updateStock, stockUpdateFields);

                    auto line_update = writeRow<OrderLine>(line, _txn);
                    auto stock_update = partialUpdateRow<Stock, std::vector<k2::String>>(updateStock, stockUpdateFields, _txn);

                    return when_all_succeed(std::move(line_update), std::move(stock_update)).discard_result();
                });
            });

            return when_all_succeed(std::move(line_updates), std::move(order_update), std::move(order_idx_update), std::move(new_order_update), std::move(district_update)).discard_result();
        });

        return when_all_succeed(std::move(main_f), std::move(customer_f), std::move(warehouse_f)).discard_result()
        .then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return _txn.end(false);
            }

            fut.ignore_ready_future();
            _total_amount *= (1 - _c_discount) * (1 + _w_tax + _d_tax);
            (void) _total_amount;
            K2LOG_D(log::tpcc, "NewOrder _total_amount: {}", _total_amount);

            return _txn.end(true);
        }).then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return make_ready_future<bool>(false);
            }

            EndResult result = fut.get0();

            if (result.status.is2xxOK() && ! _failed) {
                return make_ready_future<bool>(true);
            }

            return make_ready_future<bool>(false);
        });
    }

    static void updateStockRow(Stock& stock, const OrderLine& line, Stock& updateStock, std::vector<k2::String>& updateFields) {
        if (*(stock.Quantity) - *(line.Quantity) >= 10) {
            updateStock.Quantity = *(stock.Quantity) - *(line.Quantity);
        } else {
            updateStock.Quantity = *(stock.Quantity) + 91 - *(line.Quantity);
        }
        updateFields.push_back("Quantity");
        updateStock.YTD = *(stock.YTD) + *(line.Quantity);
        updateFields.push_back("YTD");
        updateStock.OrderCount = (*stock.OrderCount) + 1;
        updateFields.push_back("OrderCount");
        if (*(line.WarehouseID) != *(line.SupplyWarehouseID)) {
            updateStock.RemoteCount = (*stock.RemoteCount) + 1;
            updateFields.push_back("RemoteCount");
        }
    }

    void makeOrderLines() {
        _lines.reserve(*(_order.OrderLineCount));
        _order.AllLocal = 1;
        for (int32_t i = 1; i <= *(_order.OrderLineCount); ++i) {
            _lines.emplace_back(_random, _order, i, _max_w_id);
            if (*(_lines.back().SupplyWarehouseID) != _w_id) {
                _order.AllLocal = 0;
            }
        }
        uint32_t rollback = _random.UniformRandom(1, 100);
        if (rollback == 1) {
            _lines.back().ItemID = Item::InvalidID;
        }
    }

    RandomContext& _random;
    K23SIClient& _client;
    K2TxnHandle _txn;
    int16_t _w_id;
    int16_t _max_w_id;
    bool _failed;
    Order _order;
    std::vector<OrderLine> _lines;
    // The below variables are needed to "display" the order total amount,
    // but are not needed for any DB operations
    std::decimal::decimal64 _w_tax;
    std::decimal::decimal64 _d_tax;
    std::decimal::decimal64 _c_discount;
    std::decimal::decimal64 _total_amount = 0;
};

class OrderStatusT : public TPCCTxn
{
public:
    OrderStatusT(RandomContext& random, K23SIClient& client, int16_t w_id) :
                        _random(random), _client(client), _w_id(w_id) {
        _d_id = _random.UniformRandom(1, _districts_per_warehouse());
        _c_id = _random.NonUniformRandom(1023, 1, _customers_per_district()); // by last name later at

        _failed = false;
    }

    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle&& txn) {
            _txn = std::move(txn);
            return runWithTxn();
        }).handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::tpcc, exc, "Failed to start txn");
            return make_ready_future<bool>(false);
        });
    }

private:
    future<bool> runWithTxn() {
        // Get customer_id
        return getCustomerIdViaIndex()
        .then([this] () {
            // Get Customer row when get_customer_id is done
            future<> customer_f = getCustomer();

            // Get most recent order_ID when get_customer_id is done
            future<> order_f = getOrderdByCId();

            future<> order_line_f = when_all_succeed(std::move(order_f)).discard_result()
            .then([this] () {
                // Get all order-line rows when get_order is done
                return getOrderLine();
            });

            return when_all_succeed(std::move(customer_f), std::move(order_line_f)).discard_result();
        })
        // commit txn
        .then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return _txn.end(false);
            }

            fut.ignore_ready_future();
            K2LOG_D(log::tpcc, "OrderStatus txn finished");

            return _txn.end(true);
        }).then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return make_ready_future<bool>(false);
            }

            EndResult result = fut.get0();
            if (result.status.is2xxOK() && !_failed) {
                return make_ready_future<bool>(true);
            }

            return make_ready_future<bool>(false);
        });
    }

    // Get customer_id, 60% by last name, 40% by random customer id
    future<> getCustomerIdViaIndex() {
        uint32_t cid_type = _random.UniformRandom(1, 100);
        if (cid_type <= 60) {
            return make_ready_future();
        }


        return _client.createQuery(tpccCollectionName, "idx_customer_name")
        .then([this](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            k2::String lastName = _random.RandowLastNameString();
            // make Query request and set query rules.
            // 1. range:[ (_w_id, _d_id, c_last), (_w_id, _d_id, c_last) ]; 2. no record number limits; 3. forward direction scan;
            // 4. projection "c_id" field for ascending sorting;
            _query_cid = std::move(response.query);
            _query_cid.startScanRecord.serializeNext<int16_t>(_w_id);
            _query_cid.startScanRecord.serializeNext<int16_t>(_d_id);
            _query_cid.startScanRecord.serializeNext<k2::String>(lastName);
            _query_cid.endScanRecord.serializeNext<int16_t>(_w_id);
            _query_cid.endScanRecord.serializeNext<int16_t>(_d_id);
            _query_cid.endScanRecord.serializeNext<k2::String>(lastName);

            _query_cid.setLimit(-1);
            _query_cid.setReverseDirection(false);
            std::vector<String> projection{"CID"}; // make projection
            _query_cid.addProjection(projection);

            return do_with(std::vector<std::vector<dto::SKVRecord>>(), false, (uint32_t)0,
            [this] (std::vector<std::vector<dto::SKVRecord>>& result_set, bool& done, uint32_t& count) {
                return do_until(
                [this, &done] () { return done; },
                [this, &result_set, &done, &count] () {
                    return _txn.query(_query_cid)
                    .then([this, &result_set, &done, &count] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        done = response.status.is2xxOK() ? _query_cid.isDone() : true;
                        count += response.records.size();
                        result_set.push_back(std::move(response.records));

                        return make_ready_future();
                    });
                })
                .then([this, &result_set, &count] () {
                    std::vector<int32_t> cIdSort;
                    for (std::vector<dto::SKVRecord>& set : result_set) {
                        for (dto::SKVRecord& rec : set) {
                            std::optional<int32_t> cIdOpt = rec.deserializeField<int32_t>("CID");
                            cIdSort.push_back(cIdOpt.value());
                        }
                    }

                    // retrieve at the position n/2 rounded up in the sorted set from idx_customer_name table.
                    if (cIdSort.size()) {
                        std::sort(cIdSort.begin(), cIdSort.end());
                        _c_id = cIdSort[cIdSort.size() / 2];
                    }

                    return make_ready_future();
                });
            });
        });
    }

    // Get Customer row based on _w_id, _d_id, _c_id
    future<> getCustomer() {
         return _txn.read<Customer>(Customer(_w_id,_d_id,_c_id))
                    .then([this](auto&& result) mutable {
                        CHECK_READ_STATUS(result);
                        _out_c_balance = *(result.value.Balance);
                        _out_c_first_name = *(result.value.FirstName);
                        _out_c_middle_name = *(result.value.MiddleName);
                        _out_c_last_name = *(result.value.LastName);

                        return make_ready_future();
                    });
    }

    // get Order ID by Customer ID
    future<> getOIdByCIdViaIndex(){
        return _client.createQuery(tpccCollectionName, "idx_order_customer")
        .then([this](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            // make Query request and set query rules.
            // 1. range:[ (_w_id, _d_id, _c_id), (_w_id, _d_id, _c_id) ); 2. return the newest Order record;
            // 3. reverse direction scan (newest one); 4. projection "OID" field;
            _query_oid = std::move(response.query);
            _query_oid.startScanRecord.serializeNext<int16_t>(_w_id);
            _query_oid.startScanRecord.serializeNext<int16_t>(_d_id);
            _query_oid.startScanRecord.serializeNext<int32_t>(_c_id);
            _query_oid.endScanRecord.serializeNext<int16_t>(_w_id);
            _query_oid.endScanRecord.serializeNext<int16_t>(_d_id);
            _query_oid.endScanRecord.serializeNext<int32_t>(_c_id);
            _query_oid.setLimit(1);
            _query_oid.setReverseDirection(true);

            std::vector<String> projection{"OID"}; // make projection
            _query_oid.addProjection(projection);

            return do_with(std::vector<dto::SKVRecord>(), false,
            [this] (std::vector<dto::SKVRecord>& result, bool& done) {
                return do_until(
                [this, &done] () { return done; },
                [this, &result, &done] () {
                    return _txn.query(_query_oid)
                    .then([this, &result, &done] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        done = response.status.is2xxOK() ? _query_oid.isDone() : true;
                        if(response.records.size()) {
                            result.push_back(std::move(response.records[0]));
                        }

                        return make_ready_future();
                    });
                })
                .then([this, &result] () {
                    if(result.size()){
                        dto::SKVRecord& rec = result[0];
                        std::optional<int64_t> oidOpt = rec.deserializeField<int64_t>("OID");

                        _o_id = *oidOpt;
                    }

                    return make_ready_future();
                });
            });
        });
    }

    // Get order row based on _w_id, _d_id, _c_id with the largest _o_id (most recent order)
    future<> getOrderdByCId() {
        // first get _o_id using secondary index
        return getOIdByCIdViaIndex().discard_result()
                .then([this]() mutable {
                    return _txn.read<Order>(Order(_w_id,_d_id,_o_id))
                        .then([this](auto&& result) mutable {
                            CHECK_READ_STATUS(result);
                            _out_o_id = *(result.value.OrderID);
                            _out_o_entry_date = *(result.value.EntryDate);
                            _out_o_carrier_id = *(result.value.CarrierID);
                            return make_ready_future();
                        });
                });
    }

    // Get all Order-Line rows with _w_id, _d_id, _o_id
    future<> getOrderLine() {
        return _client.createQuery(tpccCollectionName, "orderline")
        .then([this](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            // make Query request and set query rules.
            // 1) all lines in certain order are returned; 2) add Projection fields as needed;
            _query_order_line = std::move(response.query);
            _query_order_line.startScanRecord.serializeNext<int16_t>(_w_id);
            _query_order_line.startScanRecord.serializeNext<int16_t>(_d_id);
            _query_order_line.startScanRecord.serializeNext<int64_t>(_o_id);
            _query_order_line.endScanRecord.serializeNext<int16_t>(_w_id);
            _query_order_line.endScanRecord.serializeNext<int16_t>(_d_id);
            _query_order_line.endScanRecord.serializeNext<int64_t>(_o_id + 1);
            _query_order_line.setLimit(-1);
            _query_order_line.setReverseDirection(false);

            std::vector<String> projection{"ItemID", "SupplyWID", "Quantity", "Amount", "DeliveryDate"}; // make projection
            _query_order_line.addProjection(projection);
            dto::expression::Expression filter{};   // make filter Expression
            _query_order_line.setFilterExpression(std::move(filter));

            return do_with(std::vector<std::vector<dto::SKVRecord>>(), false, (uint32_t)0,
            [this] (std::vector<std::vector<dto::SKVRecord>>& result_set, bool& done, uint32_t& count) {
                return do_until(
                [this, &done] () { return done; },
                [this, &result_set, &done, &count] () {
                    return _txn.query(_query_order_line)
                    .then([this, &result_set, &count, &done] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        done = response.status.is2xxOK() ? _query_order_line.isDone() : true;
                        count += response.records.size();
                        result_set.push_back(std::move(response.records));

                        return make_ready_future();
                    });
                })
                .then([this, &result_set, &count] {
                    for (auto& results : result_set) {
                        for (auto& rec : results) {
                            std::optional<int64_t> delDateOpt = rec.deserializeField<int64_t>("DeliveryDate");
                            std::optional<int32_t> itemIDOpt = rec.deserializeField<int32_t>("ItemID");
                            std::optional<int16_t> supplyWIDOpt = rec.deserializeField<int16_t>("SupplyWID");
                            std::optional<std::decimal::decimal64> amountOpt = rec.deserializeField<std::decimal::decimal64>("Amount");
                            std::optional<int16_t> quantityOpt = rec.deserializeField<int16_t>("Quantity");

                            _out_ol_delivery_date.push_back(*delDateOpt);
                            _out_ol_item_id.push_back(*itemIDOpt);
                            _out_ol_supply_wid.push_back(*supplyWIDOpt);
                            _out_ol_amount.push_back(*amountOpt);
                            _out_ol_quantity.push_back(*quantityOpt);
                        }
                    }
                    return make_ready_future();
                });
            });
        });
    }

    RandomContext& _random;
    K23SIClient& _client;
    K2TxnHandle _txn;
    bool _failed;
    int16_t _w_id;
    int16_t _d_id;
    int32_t _c_id;
    int64_t _o_id;
    Query _query_cid;
    Query _query_customer;
    Query _query_oid;
    Query _query_order;
    Query _query_order_line;

private:
    ConfigVar<int16_t> _districts_per_warehouse{"districts_per_warehouse"};
    ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};


private:
    // output data that Order-Status-Transaction wanna get
    // customer info
    std::decimal::decimal64 _out_c_balance;
    k2::String _out_c_first_name;
    k2::String _out_c_middle_name;
    k2::String _out_c_last_name;
    // order info
    int64_t _out_o_id;
    int64_t _out_o_entry_date;
    int32_t _out_o_carrier_id;
    // order lines info
    std::vector<int64_t> _out_ol_delivery_date;
    std::vector<int32_t> _out_ol_item_id;
    std::vector<int16_t> _out_ol_supply_wid;
    std::vector<std::decimal::decimal64> _out_ol_amount;
    std::vector<int16_t> _out_ol_quantity;
};

class DeliveryT : public TPCCTxn
{
public:
    DeliveryT(RandomContext& random, K23SIClient& client, int16_t w_id, uint16_t batch_size) :
                _random(random), _client(client), _batch_size(batch_size), _w_id(w_id) {
        if (_batch_size > 10) K2LOG_W(log::tpcc, "DeliveryT Ctor with batch_size bigger than 10. It should be between 1 to 10");
        for (uint16_t i = 0; i < 10; ++i){
            _d_id.push_back(_random.UniformRandom(1, _districts_per_warehouse()));
            _o_carrier_id.push_back(_random.UniformRandom(1, 10));
        }

        _failed = false;
    }

    future<bool> attempt() override {
        return do_with(
            (uint16_t)0, std::vector<uint32_t> {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            [this] (uint16_t& batchStart, std::vector<uint32_t>& vIdx) {
            return do_until(
            [this, &batchStart, &vIdx] { return batchStart >= 10; },
            [this, &batchStart, &vIdx] {
                auto startIdx = vIdx.begin() + batchStart;
                batchStart += std::min(_batch_size, (uint16_t)(10 - batchStart));
                auto endIdx = vIdx.begin() + batchStart;

                K2TxnOptions options{};
                options.deadline = Deadline(5s);
                options.priority = dto::TxnPriority::Low;
                options.syncFinalize = false;

                return _client.beginTxn(options) // begin a txn
                .then([this, &vIdx, start = startIdx, end = endIdx] (K2TxnHandle&& txn) {
                    _txn = std::move(txn);
                    return runWithTxn(start, end); // run txn
                })
                .then([this, &batchStart] {
                    K2LOG_D(log::tpcc, "DeliveryT End status: {} at {}th req of batch", _failed, batchStart);

                    // end txn
                    if (_failed) {
                        return _txn.end(false);
                    }
                    return _txn.end(true);
                })
                .then_wrapped([this] (auto&& fut) {
                    if (fut.failed()) {
                        _failed = true;
                    }
                    fut.ignore_ready_future();
                });
            })
            .then([this]() {
                if (_failed) {
                    return make_ready_future<bool>(false);
                }
                return make_ready_future<bool>(true);
            });
        });
    }

private:
    future<> runWithTxn(std::vector<uint32_t>::iterator start, std::vector<uint32_t>::iterator end) {
        return parallel_for_each(start, end, [this](uint32_t& idx) {
            // get lowest NO_O_ID row in NEW-ORDER table with matching w_id and d_id
            return getOrderID(idx)
            .then([this, &idx] (bool success) {
                if (!success) {
                    return make_ready_future();
                }

                // when we got NO_O_ID, delete the selected row in NEW-ORDER table
                future<> delNORow_f = delNewOrderRow(idx);

                // when we got NO_O_ID, update the carrierID in ORDER table
                future<> updateCarrierID_f = updateCarrierID(idx);

                // when we got NO_O_ID, retrieve the "customerID" and "orderLineCount" in ORDER table
                future<> updates_f = getCustomerID(idx)
                .then([this, idx] {
                    // (1) when we got "orderLineCount", all rows in the ORDER_LINE table with mathing w_id, d_id and
                    //     o_id are seclected, get the sum of all OL_AMOUNT;
                    // (2) when we got "sum of all OL_AMOUNT" and "customerID", update the row in the CUSTOMER table.
                    future<> updateCustomerRow_f = getOLSumAmount(idx)
                    .then([this, idx] {
                        return updateCustomerRow(idx);
                    });

                    // when we got "orderLineCount", update all the "DeliveryDates" in ORDER LINE table
                    // with matching w_id, d_id and o_id.
                    future<> updateOLDeliveryD_f = updateOLDeliveryD(idx);

                    return when_all_succeed(std::move(updateOLDeliveryD_f), std::move(updateCustomerRow_f))
                    .then_wrapped([this] (auto&& fut) {
                        if (fut.failed()) {
                            _failed = true;
                            fut.ignore_ready_future(); // May fail expectedly if there are no new orders
                        }
                        return make_ready_future();
                    });
                });

                return when_all_succeed(std::move(updates_f), std::move(updateCarrierID_f), std::move(delNORow_f))
                .then_wrapped([this, idx] (auto&& fut) {
                    if (fut.failed()) {
                        _failed = true;
                        fut.ignore_ready_future(); // May fail expectedly if there are no new orders
                    }
                    return make_ready_future();
                });
            });
        });
    }

    // get lowest NO_O_ID row in NEW-ORDER table with matching _w_id and _d_id
    // Todo: do the whole batch size in one query
    future<bool> getOrderID(uint32_t idx) {
        return _client.createQuery(tpccCollectionName, "neworder")
        .then([this, idx](auto&& response) mutable {
            if (!response.status.is2xxOK()) {
                K2LOG_I(log::tpcc, "DeliveryT getOrderID createQuery failed, status: {}", response.status);
                _failed = true;
                return make_ready_future<bool>(false);
            }

            return  do_with(std::move(response.query), [this, idx] (Query& query_oid) {
                // make Query request and set Query rules.
                // 1) just return the lowest SKVRecord; 2) add Projection fields (i.e. oid) as needed;
                query_oid.startScanRecord.serializeNext<int16_t>(_w_id);
                query_oid.startScanRecord.serializeNext<int16_t>(_d_id[idx]);
                query_oid.startScanRecord.serializeNext<int64_t>(_saved_noid);
                query_oid.endScanRecord.serializeNext<int16_t>(_w_id);
                query_oid.endScanRecord.serializeNext<int16_t>(_d_id[idx] + 1);
                query_oid.setLimit(1);
                query_oid.setReverseDirection(false);

                std::vector<String> projection{"OID"};  // make projection
                query_oid.addProjection(projection);
                dto::expression::Expression filter{};   // make filter expression
                query_oid.setFilterExpression(std::move(filter));

                return _txn.query(query_oid)
                .then([this, idx, &query_oid] (auto&& response) mutable {
                    if (!response.status.is2xxOK()) {
                        K2LOG_I(log::tpcc, "DeliveryT getOrderID query response Error, status: {}", response.status);
                        _failed = true;
                        return make_ready_future<bool>(false);
                    }

                    if (response.records.size()) {
                        dto::SKVRecord& rec = response.records[0];
                        // get order id
                        std::optional<int64_t> orderIDOpt = rec.deserializeField<int64_t>("OID");
                        K2ASSERT(log::tpcc, orderIDOpt, "DeliveryT getOrderID() retrieved null value.");
                        _NO_O_ID[idx] = *orderIDOpt;
                        _saved_noid = *orderIDOpt;

                        return make_ready_future<bool>(true);
                    } else {
                        K2LOG_I(log::tpcc, "DeliveryT parallel request [{}]: not found Order ID "
                                "matches w_id:{}, d_id:{}", idx, _w_id, _d_id[idx]);
                        // set all output values to -1
                        _NO_O_ID[idx] = -1;
                        _O_C_ID[idx] = -1;
                        _OL_SUM_AMOUNT[idx] = -1;

                        // report MISS_condition occurs more than once of the delivery transaction
                        if (_miss_once) {
                            K2LOG_I(log::tpcc, "TPCC DeliveryT MISS_Condition: occurs more than once! not Found "
                                    "Order ID matches w_id[{}] & d_id[{}] in New-Order table", _w_id, _d_id[idx]);
                        }
                        _miss_once = true;

                        return make_ready_future<bool>(false);
                    }
                });
            });
        });
    }

    // delete the selected row in NEW-ORDER table
    future<> delNewOrderRow(uint32_t idx) {
        NewOrder delNORow(_w_id, _d_id[idx], _NO_O_ID[idx]);
        return writeRow<NewOrder>(delNORow, _txn, true).discard_result();
    }

    // retrieve the "LineCount" and "CID" in ORDER table with matching _w_id, _d_id and _no_o_id
    future<> getCustomerID(uint32_t idx) {
        return _client.createQuery(tpccCollectionName, "order")
        .then([this, idx] (auto&& response) mutable {
            CHECK_READ_STATUS(response);

            // only retrieve "LineCount" and "CID" field in ORDER table, use a Query instead of a Read
            return do_with(std::move(response.query), [this, idx] (Query& query_cid) {
                // make Query request and set query rules.
                // (1) add projection fields (i.e. LineCount, CID) as needed;
                query_cid.startScanRecord.serializeNext<int16_t>(_w_id);
                query_cid.startScanRecord.serializeNext<int16_t>(_d_id[idx]);
                query_cid.startScanRecord.serializeNext<int64_t>(_NO_O_ID[idx]);
                query_cid.endScanRecord.serializeNext<int16_t>(_w_id);
                query_cid.endScanRecord.serializeNext<int16_t>(_d_id[idx]);
                query_cid.endScanRecord.serializeNext<int64_t>(_NO_O_ID[idx]);
                query_cid.setLimit(1);
                query_cid.setReverseDirection(false);

                std::vector<String> projection{"OrderLineCount", "CID"};  // make projection
                query_cid.addProjection(projection);
                dto::expression::Expression filter{};   // make filter Expression
                query_cid.setFilterExpression(std::move(filter));

                return _txn.query(query_cid)
                .then([this, idx] (auto&& response) {
                    CHECK_READ_STATUS(response);
                    if (response.records.size() == 0) {
                        return make_exception_future<>(std::runtime_error("Order not found"));
                    }

                    // get customer ID
                    dto::SKVRecord& rec = response.records[0];
                    std::optional<int16_t> lineCountOpt = rec.deserializeField<int16_t>("OrderLineCount");
                    std::optional<int32_t> cidOpt = rec.deserializeField<int32_t>("CID");
                    _o_line_count[idx] = *lineCountOpt;
                    _O_C_ID[idx] = *cidOpt;

                    return make_ready_future();
                });
            });
        });
    }

    // update the carrierID in ORDER table
    future<> updateCarrierID(uint32_t idx) {
        std::vector<String> updateFields({"CarrierID"});

        Order updateOrder(_w_id, _d_id[idx], _NO_O_ID[idx]);
        updateOrder.CarrierID = _o_carrier_id[idx];

        return partialUpdateRow<Order, std::vector<String>>(updateOrder, updateFields, _txn).discard_result();
    }

    // get the sum of all OL_AMOUNT in the ORDER_LINE table with mathing w_id, d_id, o_id
    future<> getOLSumAmount(uint32_t idx) {
        return _client.createQuery(tpccCollectionName, "orderline")
        .then([this, idx](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            return do_with(
            std::move(response.query),
            std::vector<std::vector<dto::SKVRecord>>(),
            false,
            (uint32_t) 0,
            [this, idx] (Query& query_order_line, std::vector<std::vector<dto::SKVRecord>>& result_set,
                    bool& done, uint32_t& count) {
                // make Query request and set query rules.
                // 1) add Projection fields (i.e. amount) as needed;
                query_order_line.startScanRecord.serializeNext<int16_t>(_w_id);
                query_order_line.startScanRecord.serializeNext<int16_t>(_d_id[idx]);
                query_order_line.startScanRecord.serializeNext<int64_t>(_NO_O_ID[idx]);
                query_order_line.startScanRecord.serializeNext<int32_t>(1);
                query_order_line.endScanRecord.serializeNext<int16_t>(_w_id);
                query_order_line.endScanRecord.serializeNext<int16_t>(_d_id[idx]);
                query_order_line.endScanRecord.serializeNext<int64_t>(_NO_O_ID[idx]);
                query_order_line.endScanRecord.serializeNext<int32_t>(_o_line_count[idx] + 1);
                query_order_line.setLimit(_o_line_count[idx]);
                query_order_line.setReverseDirection(false);

                std::vector<String> projection{"Amount"};   // make projection
                query_order_line.addProjection(projection);
                dto::expression::Expression filter{};       // make filter Expression
                query_order_line.setFilterExpression(std::move(filter));

                return do_until(
                [this, &done] () { return done; },
                [this, &query_order_line, &result_set, &done, &count, idx] () {
                    return _txn.query(query_order_line)
                    .then([this, &query_order_line,  &result_set, &done, &count, idx] (auto&& response) {
                        CHECK_READ_STATUS(response);

                        done = response.status.is2xxOK() ? query_order_line.isDone() : true;
                        count += response.records.size();
                        result_set.push_back(std::move(response.records));

                        return make_ready_future();
                    });
                })
                .then([this, &result_set, &count, idx] () {
                    for (std::vector<dto::SKVRecord>& set : result_set) {
                        for (dto::SKVRecord& rec : set) {
                            std::optional<std::decimal::decimal64> amountOpt = rec.deserializeField<std::decimal::decimal64>("Amount");
                            _OL_SUM_AMOUNT[idx] += *amountOpt;
                        }
                    }

                    return make_ready_future();
                });
            });
        });
    }

    // update the OL_DELIVERY_D, the delivery dates, to the current system time in the ORDER_LINE table
    future<> updateOLDeliveryD(uint32_t idx) {
        std::vector<int32_t> LineNumber;
        LineNumber.reserve(_o_line_count[idx]);
        for (int32_t i = 1; i <= _o_line_count[idx]; ++i) {
            LineNumber.emplace_back(i);
        }

        return parallel_for_each(LineNumber.begin(), LineNumber.end(), [this, idx] (int32_t& lineNum) {
            OrderLine updateOLRow(_w_id, _d_id[idx], _NO_O_ID[idx], lineNum);
            updateOLRow.DeliveryDate = getDate();

            std::vector<String> OLUpdateFields{"DeliveryDate"};

            return partialUpdateRow<OrderLine, std::vector<String>>(updateOLRow, OLUpdateFields, _txn).discard_result();
        });
    }

    // update the row in the CUSTOMER table
    future<> updateCustomerRow(uint32_t idx) {
        return _client.createQuery(tpccCollectionName, "customer")
        .then([this, &idx] (auto&& response) mutable {
            CHECK_READ_STATUS(response);

            return do_with(std::move(response.query), [this, idx] (Query& query_customer) {
                // make Query request and make projection
                query_customer.startScanRecord.serializeNext<int16_t>(_w_id);
                query_customer.startScanRecord.serializeNext<int16_t>(_d_id[idx]);
                query_customer.startScanRecord.serializeNext<int32_t>(_O_C_ID[idx]);
                query_customer.endScanRecord.serializeNext<int16_t>(_w_id);
                query_customer.endScanRecord.serializeNext<int16_t>(_d_id[idx]);
                query_customer.endScanRecord.serializeNext<int32_t>(_O_C_ID[idx]);
                query_customer.setLimit(1);
                query_customer.setReverseDirection(false);

                std::vector<String> projection{"Balance", "DeliveryCount"}; // make projection
                query_customer.addProjection(projection);
                dto::expression::Expression filter{}; // make filter Expression
                query_customer.setFilterExpression(std::move(filter));

                return _txn.query(query_customer)
                .then([this, idx] (auto&& response) {
                    CHECK_READ_STATUS(response);
                    if (response.records.size() == 0) {
                        return make_exception_future<>(std::runtime_error("Customer not found"));
                    }

                    dto::SKVRecord& rec = response.records[0];
                    std::optional<std::decimal::decimal64> balanceOpt = rec.deserializeField<std::decimal::decimal64>("Balance");
                    std::optional<int32_t> deliveryCountOpt = rec.deserializeField<int32_t>("DeliveryCount");

                    Customer updateCustomer(_w_id, _d_id[idx], _O_C_ID[idx]);
                    updateCustomer.Balance = *balanceOpt + _OL_SUM_AMOUNT[idx];
                    updateCustomer.DeliveryCount = *deliveryCountOpt + 1;

                    std::vector<String> updateCustomerFileds{"Balance", "DeliveryCount"};

                    return partialUpdateRow<Customer, std::vector<String>>
                            (updateCustomer, updateCustomerFileds, _txn).discard_result();
                });
            });
        });
    }

private:
    RandomContext& _random;
    K23SIClient& _client;
    K2TxnHandle _txn;
    bool _failed;
    bool _miss_once = false;
    uint16_t _batch_size; // range from 1-10 is allowed

    int16_t _w_id;
    std::vector<int16_t> _d_id;
    std::vector<int32_t> _o_carrier_id;
    std::vector<int16_t> _o_line_count = std::vector<int16_t>(10, -1); // Count how many Order Lines for each Order
    ConfigVar<int16_t> _districts_per_warehouse{"districts_per_warehouse"};

private:
    // output data that DeliveryT wanna get
    std::vector<int64_t> _NO_O_ID = std::vector<int64_t>(10, -1); // NEW-ORDER table info: order ID
    std::vector<int32_t> _O_C_ID = std::vector<int32_t>(10, -1); // ORDER table info: customer ID
    std::vector<std::decimal::decimal64> _OL_SUM_AMOUNT = std::vector<std::decimal::decimal64>(10, -1); // ORDER-LINE table info: sum of all amount

    // Save the next expected new order id to avoid scans over deleted records
    static inline int64_t _saved_noid = 0;
};

class StockLevelT : public TPCCTxn
{
public:
    StockLevelT(RandomContext& random, K23SIClient& client, int16_t w_id, int16_t d_id) :
                        _random(random), _client(client), _w_id(w_id), _d_id(d_id) {
        _threshold = _random.UniformRandom(10, 20);
        _low_stock = 0;

        _failed = false;
    }

    future<bool> attempt() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle&& txn) {
            _txn = std::move(txn);
            return runWithTxn();
        }).handle_exception([] (auto exc) {
            K2LOG_W_EXC(log::tpcc, exc, "Failed to start txn");
            return make_ready_future<bool>(false);
        });
    }

private:
    future<bool> runWithTxn() {
        return getNextOrder()
        .then([this] (int64_t d_next_order_id) {
            K2LOG_D(log::tpcc, "StockLevel txn got next order no = {}", d_next_order_id);
            return getItemIDs(d_next_order_id);
        })
        .then([this] (std::set<int32_t> && item_ids){
            K2LOG_D(log::tpcc, "StockLevel txn got item_ids");
            return getStockQuantity(std::move(item_ids));
        })
        // commit txn
        .then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return _txn.end(false);
            }

            fut.ignore_ready_future();
            K2LOG_D(log::tpcc, "StockLevel txn finished with lowstock = {}",_low_stock);

            return _txn.end(true);
        }).then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return make_ready_future<bool>(false);
            }

            EndResult result = fut.get0();
            if (result.status.is2xxOK() && !_failed) {
                return make_ready_future<bool>(true);
            }

            return make_ready_future<bool>(false);
        });
    }

    // Get Next order id from District
    future< int64_t > getNextOrder() {
        return _txn.read<District>(District(_w_id,_d_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS_TYPE(result,int64_t);
            int64_t d_next_order_id = *(result.value.NextOrderID);
            return make_ready_future<int64_t>(d_next_order_id);
        });
    }

    // get all itemids from Orderline with _w_id, _d_id, order id in [_d_next_order_id-20,_d_next_order_id-1]
    future< std::set<int32_t> > getItemIDs(int64_t d_next_order_id) {
        return _client.createQuery(tpccCollectionName, "orderline")
        .then([this, &d_next_order_id](auto&& response) mutable {
            CHECK_READ_STATUS_TYPE(response, std::set<int32_t>);
            // make Query request and set query rules
            // range: [(_w_id, _d_id, _d_next_order_id-20, 0), (_w_id, _d_id, _d_next_order_id-1, 0)];
            _query_orderline = std::move(response.query);
            _query_orderline.startScanRecord.serializeNext<int16_t>(_w_id);
            _query_orderline.startScanRecord.serializeNext<int16_t>(_d_id);
            _query_orderline.startScanRecord.serializeNext<int64_t>(d_next_order_id-20);
            _query_orderline.endScanRecord.serializeNext<int16_t>(_w_id);
            _query_orderline.endScanRecord.serializeNext<int16_t>(_d_id);
            _query_orderline.endScanRecord.serializeNext<int64_t>(d_next_order_id);
            _query_orderline.setLimit(-1);
            _query_orderline.setReverseDirection(false);

            std::vector<String> projection{"ItemID"}; // make projection
            _query_orderline.addProjection(projection);
            dto::expression::Expression filter{};   // make filter Expression
            _query_orderline.setFilterExpression(std::move(filter));

            return do_with(std::vector<std::vector<dto::SKVRecord>>(), false,
            [this] (std::vector<std::vector<dto::SKVRecord>>& result_set, bool& done) {
                return do_until(
                [this, &done] () { return done; },
                [this, &result_set, &done] () {
                    return _txn.query(_query_orderline)
                    .then([this, &result_set, &done] (auto&& response) {
                        CHECK_READ_STATUS(response);
                        done = response.status.is2xxOK() ? _query_orderline.isDone() : true;
                        result_set.push_back(std::move(response.records));

                        return make_ready_future();
                    });
                })
                .then([this, &result_set] () {
                    std::set<int32_t> item_ids;
                    for (std::vector<dto::SKVRecord>& set : result_set) {
                        for (dto::SKVRecord& rec : set) {
                            std::optional<int32_t> itemId = rec.deserializeField<int32_t>("ItemID");
                            item_ids.insert(*itemId);
                        }
                    }

                    return make_ready_future< std::set<int32_t> >(std::move(item_ids));
                });
            });
        });
    }

    // get stock quantity for each item and update low stock
    future<> getStockQuantity(std::set<int32_t>&& item_ids) {
        return parallel_for_each(item_ids.begin(), item_ids.end(), [this, &item_ids] (int32_t item_id) {
            return _txn.read<Stock>(Stock(_w_id,item_id))
            .then([this] (auto&& result) {
                CHECK_READ_STATUS(result);
                if ( (*(result.value.Quantity)) < _threshold) {
                    _low_stock++;
                }
                return make_ready_future();
            });
        });
    }

    RandomContext& _random;
    K23SIClient& _client;
    K2TxnHandle _txn;
    bool _failed;
    int16_t _w_id;
    int16_t _d_id;
    int16_t _threshold;
    Query _query_orderline;

private:
    // output data that Stock-Level-Transaction wants
    // _low_stock count
    int64_t _low_stock;
};
