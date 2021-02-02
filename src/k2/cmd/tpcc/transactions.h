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
#include <seastar/core/sleep.hh>

#include "schema.h"
#include "Log.h"

using namespace seastar;
using namespace k2;


struct CIdSortElement {
    String firstName;
    int32_t c_id;
    bool operator<(const CIdSortElement& other) const noexcept {
        return firstName < other.firstName;
    }
};

class AtomicVerify;

class TPCCTxn {
public:
    virtual future<bool> run() = 0;
    virtual ~TPCCTxn() = default;
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
    }

    future<bool> run() override {
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
        future<> CId_get = getCIdByLastName();

        return when_all_succeed(std::move(warehouse_update), std::move(district_update), std::move(CId_get)).discard_result()
        .then([this] () {
            future<> history_update = historyUpdate();
            future<> customer_update = customerUpdate();

            return when_all_succeed(std::move(customer_update), std::move(history_update)).discard_result()
            .then_wrapped([this] (auto&& fut) {
                if (fut.failed()) {
                    _failed = true;
                    fut.ignore_ready_future();
                    return _txn.end(false);
                }

                fut.ignore_ready_future();
                K2LOG_D(log::tpcc, "Payment txn finished");

                return _txn.end(!_abort);
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
                memcpy((char*)customer.Info->c_str() + shift_size, (char*)result.value.Info->c_str(), 500-shift_size);
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
    // 60% select customer by last name; 40%, the _c_id has already been set randomly at the consturctor
    future<> getCIdByLastName() {
        uint32_t cid_type = _random.UniformRandom(1, 100);
        if (cid_type > 60) {
            return make_ready_future();
        }

        k2::String lastName = _random.RandowLastNameString();

        return _client.createQuery(tpccCollectionName, "customer")
        .then([this, &lastName](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            // make Query request and set query rules.
            // 1. range:[ (_w_id, _d_id, 0), (_w_id, _d_id + 1, 0) ); 2. no record number limits; 3. forward direction scan;
            // 4. projection "FirstName" field for ascending sorting; 5. filter out the record rows of lastName
            _query = std::move(response.query);
            _query.startScanRecord.serializeNext<int16_t>(_c_w_id);
            _query.startScanRecord.serializeNext<int16_t>(_c_d_id);
            _query.endScanRecord.serializeNext<int16_t>(_c_w_id);
            _query.endScanRecord.serializeNext<int16_t>(_c_d_id + 1);

            _query.setLimit(-1);
            _query.setReverseDirection(false);
            std::vector<String> projection{"CID", "FirstName"}; // make projection
            _query.addProjection(projection);
            std::vector<dto::expression::Value> values; // make filter Expression
            std::vector<dto::expression::Expression> exps;
            values.emplace_back(dto::expression::makeValueReference("LastName"));
            values.emplace_back(dto::expression::makeValueLiteral<k2::String>(std::move(lastName)));
            dto::expression::Expression filter = dto::expression::makeExpression(dto::expression::Operation::EQ,
                                                    std::move(values), std::move(exps));
            _query.setFilterExpression(std::move(filter));

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
                    std::vector<CIdSortElement> cIdSort;
                    for (std::vector<dto::SKVRecord>& set : result_set) {
                        for (dto::SKVRecord& rec : set) {
                            std::optional<int32_t> cIdOpt = rec.deserializeField<int32_t>("CID");
                            std::optional<String> firstNameOpt = rec.deserializeField<String>("FirstName");

                            cIdSort.push_back(CIdSortElement{*firstNameOpt, *cIdOpt});
                        }
                    }

                    // sorted by C_FIRST_NAME in ascending order, and retrieve at the
                    // position n/2 rounded up in the sorted set from CUSTORMER table.
                    if (cIdSort.size()) {
                        std::sort(cIdSort.begin(), cIdSort.end());
                        _c_id = cIdSort[cIdSort.size() / 2].c_id;
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

private:
    ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
    ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};

friend class AtomicVerify;
};

class NewOrderT : public TPCCTxn
{
public:
    NewOrderT(RandomContext& random, K23SIClient& client, int16_t w_id, int16_t max_w_id) :
                        _random(random), _client(client), _w_id(w_id), _max_w_id(max_w_id),
                        _failed(false), _order(random, w_id) {}

    future<bool> run() override {
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

            return when_all_succeed(std::move(line_updates), std::move(order_update), std::move(new_order_update), std::move(district_update)).discard_result();
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
        for (int32_t i = 0; i < *(_order.OrderLineCount); ++i) {
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

    future<bool> run() override {
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
        return getCustomerId()
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
    future<> getCustomerId() {
        uint32_t cid_type = _random.UniformRandom(1, 100);
        // 40%, the _c_id has already been set randomly at the constructor
        if (cid_type > 60) {
            return make_ready_future();
        } //else{}, 60% select customer by last name

        k2::String lastName = _random.RandowLastNameString();

        return _client.createQuery(tpccCollectionName, "customer")
        .then([this, &lastName](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            // make Query request and set query rules.
            // 1. range:[ (_w_id, _d_id, 0), (_w_id, _d_id + 1, 0) ); 2. no record number limits; 3. forward direction scan;
            // 4. projection "FirstName" field for ascending sorting; 5. filter out the record rows of lastName
            _query_cid = std::move(response.query);
            _query_cid.startScanRecord.serializeNext<int16_t>(_w_id);
            _query_cid.startScanRecord.serializeNext<int16_t>(_d_id);
            _query_cid.endScanRecord.serializeNext<int16_t>(_w_id);
            _query_cid.endScanRecord.serializeNext<int16_t>(_d_id + 1);
            _query_cid.setLimit(-1);
            _query_cid.setReverseDirection(false);

            std::vector<String> projection{"CID", "FirstName"}; // make projection
            _query_cid.addProjection(projection);
            std::vector<dto::expression::Value> values; // make filter Expression
            std::vector<dto::expression::Expression> exps;
            values.emplace_back(dto::expression::makeValueReference("LastName"));
            values.emplace_back(dto::expression::makeValueLiteral<String>(std::move(lastName)));
            dto::expression::Expression filter = dto::expression::makeExpression(dto::expression::Operation::EQ,
                                                    std::move(values), std::move(exps));
            _query_cid.setFilterExpression(std::move(filter));

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
                    std::vector<CIdSortElement> cIdSort;
                    for (std::vector<dto::SKVRecord>& set : result_set) {
                        for (dto::SKVRecord& rec : set) {
                            std::optional<int32_t> cIdOpt = rec.deserializeField<int32_t>("CID");
                            std::optional<String> firstNameOpt = rec.deserializeField<String>("FirstName");

                            cIdSort.push_back(CIdSortElement{*firstNameOpt, *cIdOpt});
                        }
                    }

                    // sorted by C_FIRST_NAME in ascending order, and retrieve at the
                    // position n/2 rounded up in the sorted set from CUSTORMER table.
                    if (cIdSort.size()) {
                        std::sort(cIdSort.begin(), cIdSort.end());
                        _c_id = cIdSort[cIdSort.size() / 2].c_id;
                    }

                    return make_ready_future();
                });
            });
        });
    }

    // Get Customer row based on _w_id, _d_id, _c_id
    future<> getCustomer() {
        return _client.createQuery(tpccCollectionName, "customer")
        .then([this](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            // make Query request and set query rules.
            // 1) just one skvrecord returns; 2) add Projection fields as needed;
            _query_customer = std::move(response.query);
            _query_customer.startScanRecord.serializeNext<int16_t>(_w_id);
            _query_customer.startScanRecord.serializeNext<int16_t>(_d_id);
            _query_customer.startScanRecord.serializeNext<int32_t>(_c_id);
            _query_customer.endScanRecord.serializeNext<int16_t>(_w_id);
            _query_customer.endScanRecord.serializeNext<int16_t>(_d_id);
            _query_customer.endScanRecord.serializeNext<int32_t>(_c_id + 1);
            _query_customer.setLimit(-1);
            _query_customer.setReverseDirection(false);

            std::vector<String> projection{"Balance", "FirstName", "MiddleName", "LastName"}; // make projection
            _query_customer.addProjection(projection);
            dto::expression::Expression filter{};   // make filter Expression
            _query_customer.setFilterExpression(std::move(filter));

            return _txn.query(_query_customer)
            .then([this] (auto&& response) {
                CHECK_READ_STATUS(response);

                dto::SKVRecord& rec = response.records[0];
                std::optional<std::decimal::decimal64> balanceOpt = rec.deserializeField<std::decimal::decimal64>("Balance");
                std::optional<String> firstNameOpt = rec.deserializeField<String>("FirstName");
                std::optional<String> middleNameOpt = rec.deserializeField<String>("MiddleName");
                std::optional<String> lastNameOpt = rec.deserializeField<String>("LastName");

                _out_c_balance = *balanceOpt;
                _out_c_first_name = *firstNameOpt;
                _out_c_middle_name = *middleNameOpt;
                _out_c_last_name = *lastNameOpt;
                return make_ready_future();
            });
        });
    }

    // Get order row based on _w_id, _d_id, _c_id with the largest _o_id (most recent order)
    future<> getOrderdByCId() {
        return _client.createQuery(tpccCollectionName, "order")
        .then([this](auto&& response) mutable {
            CHECK_READ_STATUS(response);

            // make Query request and set query rules.
            // 1. range:[ (_w_id, _d_id, INT64_MAX), (_w_id, _d_id-1, INT64_MAX) ); 2. return the newest Order record;
            // 3. reverse direction scan (newest one); 4. projection "OID", "EntryDate", "CarrierID" fields;
            // 5. filter out the record rows of customer id
            _query_order = std::move(response.query);
            _query_order.startScanRecord.serializeNext<int16_t>(_w_id);
            _query_order.startScanRecord.serializeNext<int16_t>(_d_id);
            _query_order.startScanRecord.serializeNext<int64_t>(INT64_MAX);
            _query_order.endScanRecord.serializeNext<int16_t>(_w_id);
            _query_order.endScanRecord.serializeNext<int16_t>(_d_id - 1);
            _query_order.endScanRecord.serializeNext<int64_t>(INT64_MAX);
            _query_order.setLimit(1);
            _query_order.setReverseDirection(true);

            std::vector<String> projection{"OID", "EntryDate", "CarrierID"}; // make projection
            _query_order.addProjection(projection);
            std::vector<dto::expression::Value> values; // make filter Expression
            std::vector<dto::expression::Expression> exps;
            values.emplace_back(dto::expression::makeValueReference("CID"));
            values.emplace_back(dto::expression::makeValueLiteral<int16_t>(std::move(_c_id)));
            dto::expression::Expression filter = dto::expression::makeExpression(dto::expression::Operation::EQ,
                                                    std::move(values), std::move(exps));
            _query_order.setFilterExpression(std::move(filter));

            return _txn.query(_query_order)
            .then([this] (auto&& response) {
                CHECK_READ_STATUS(response);

                for (dto::SKVRecord& rec : response.records) {
                    std::optional<int64_t> oidOpt = rec.deserializeField<int64_t>("OID");
                    std::optional<int64_t> entryDateOpt = rec.deserializeField<int64_t>("EntryDate");
                    std::optional<int32_t> carrierOpt = rec.deserializeField<int32_t>("CarrierID");

                    _o_id = *oidOpt;
                    _out_o_id = *oidOpt;
                    _out_o_entry_date = *entryDateOpt;
                    _out_o_carrier_id = *carrierOpt;
                }
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
    Query _query_order;
    Query _query_order_line;

private:
    ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
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
