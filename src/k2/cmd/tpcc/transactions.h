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

using namespace seastar;
using namespace k2;

class AtomicVerify;

class TPCCTxn {
public:
    virtual future<bool> run() = 0;
    virtual ~TPCCTxn() = default;
};

class PaymentT : public TPCCTxn
{
public:
    PaymentT(RandomContext& random, K23SIClient& client, uint32_t w_id, uint32_t max_w_id) :
                        _client(client), _w_id(w_id) {

        _d_id = random.UniformRandom(1, _districts_per_warehouse());
        _c_id = random.NonUniformRandom(1023, 1, _customers_per_district()); // TODO, by last name
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
            K2WARN_EXC("Failed to start txn: ", exc);
            return make_ready_future<bool>(false);
        });
    }

private:
    future<bool> runWithTxn() {
        future<> warehouse_update = warehouseUpdate();
        future<> district_update = districtUpdate();
        future<> customer_update = customerUpdate();

        future<> history_update = when_all_succeed(std::move(warehouse_update), std::move(district_update))
        .then([this] () {
            return historyUpdate();
        });

        return when_all_succeed(std::move(customer_update), std::move(history_update))
        .then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return _txn.end(false);
            }

            fut.ignore_ready_future();
            K2DEBUG("Payment txn finished");

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
    }

    future<> warehouseUpdate() {
        return _txn.read<Warehouse>(Warehouse(_w_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);
            *(result.value.YTD) += _amount;
            _w_name = *(result.value.Name);
            return writeRow<Warehouse>(result.value, _txn).discard_result();
        });
    }

    future<> districtUpdate() {
        return _txn.read<District>(District(_w_id, _d_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);
            *(result.value.YTD) += _amount;
            _d_name = *(result.value.Name);
            return writeRow<District>(result.value, _txn).discard_result();
        });
    }

    future<> customerUpdate() {
        return _txn.read<Customer>(Customer(_c_w_id, _c_d_id, _c_id))
        .then([this] (auto&& result) {
            CHECK_READ_STATUS(result);

            *(result.value.Balance) -= _amount;
            *(result.value.YTDPayment) += _amount;
            (*(result.value.PaymentCount))++;

            if (*(result.value.Credit) == "BC") {
                size_t shift_size = sizeof(_c_id) + sizeof(_c_d_id) + sizeof(_d_id) + sizeof(_w_id) + sizeof(_amount);
                memmove((char*)result.value.Info->c_str() + shift_size, 
                    (char*)result.value.Info->c_str(), 500-shift_size);
                uint32_t offset = 0;
                memcpy((char*)result.value.Info->c_str() + offset, &_c_id, sizeof(_c_id));
                offset += sizeof(_c_id);
                memcpy((char*)result.value.Info->c_str() + offset, &_c_d_id, sizeof(_c_d_id));
                offset += sizeof(_c_d_id);
                memcpy((char*)result.value.Info->c_str() + offset, &_d_id, sizeof(_d_id));
                offset += sizeof(_d_id);
                memcpy((char*)result.value.Info->c_str() + offset, &_w_id, sizeof(_w_id));
                offset += sizeof(_w_id);
                memcpy((char*)result.value.Info->c_str() + offset, &_amount, sizeof(_amount));
            }

            return writeRow<Customer>(result.value, _txn).discard_result();
        });
    }

    future<> historyUpdate() {
        History history(_w_id, _d_id, _c_id, _c_w_id, _c_d_id, _amount, _w_name.c_str(), _d_name.c_str());
        return writeRow<History>(history, _txn).discard_result();
    }

    K23SIClient& _client;
    K2TxnHandle _txn;
    uint32_t _w_id;
    uint32_t _c_w_id;
    uint32_t _c_id;
    uint32_t _amount;
    uint16_t _d_id;
    uint16_t _c_d_id;
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
    NewOrderT(RandomContext& random, K23SIClient& client, uint32_t w_id, uint32_t max_w_id) :
                        _random(random), _client(client), _w_id(w_id), _max_w_id(max_w_id), _failed(false), _order(random, w_id) {}

    future<bool> run() override {
        K2TxnOptions options{};
        options.deadline = Deadline(5s);
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle&& txn) {
            _txn = std::move(txn);
            return runWithTxn();
        }).handle_exception([] (auto exc) {
            K2WARN_EXC("Failed to start txn: ", exc);
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
            (*(result.value.NextOrderID))++;
            future<WriteResult> district_update = writeRow<District>(result.value, _txn);

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
                        K2DEBUG("Bad read status: " << result.status);
                        return make_exception_future<Item>(std::runtime_error("Bad read status"));
                    }

                    return make_ready_future<Item>(std::move(result.value));

                }).then([this, supply_id=*(line.SupplyWarehouseID)] (Item&& item) {
                    return _txn.read<Stock>(Stock(supply_id, *(item.ItemID)))
                    .then([item, supply_id] (auto&& result) {
                        if (!result.status.is2xxOK()) {
                            K2DEBUG("Bad read status: " << result.status);
                            return make_exception_future<std::pair<Item, Stock>>(std::runtime_error("Bad read status"));
                        }
                        return make_ready_future<std::pair<Item, Stock>>(std::make_pair(std::move(item), result.value));
                    });

                }).then([this, line] (std::pair<Item, Stock>&& pair) mutable {
                    auto& [item, stock] = pair;
                    *(line.Amount) = *(item.Price) * *(line.Quantity);
                    _total_amount += *(line.Amount);
                    line.DistInfo = stock.getDistInfo(*(line.DistrictID));
                    updateStockRow(stock, line);

                    auto line_update = writeRow<OrderLine>(line, _txn);
                    auto stock_update = writeRow<Stock>(stock, _txn);

                    return when_all_succeed(std::move(line_update), std::move(stock_update)).discard_result();
                });
            });

            return when_all_succeed(std::move(line_updates), std::move(order_update), std::move(new_order_update), std::move(district_update)).discard_result();
        });

        return when_all_succeed(std::move(main_f), std::move(customer_f), std::move(warehouse_f))
        .then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                _failed = true;
                fut.ignore_ready_future();
                return _txn.end(false);
            }

            fut.ignore_ready_future();
            _total_amount *= (1 - _c_discount) * (1 + _w_tax + _d_tax);
            (void) _total_amount;
            K2DEBUG("NewOrder _total_amount: " << _total_amount);

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

    static void updateStockRow(Stock& stock, const OrderLine& line) {
        if (*(stock.Quantity) - *(line.Quantity) >= 10) {
            *(stock.Quantity) -= *(line.Quantity);
        } else {
            *(stock.Quantity) = *(stock.Quantity) + 91 - *(line.Quantity);
        }
        *(stock.YTD) += *(line.Quantity);
        (*(stock.OrderCount))++;
        if (*(line.WarehouseID) != *(line.SupplyWarehouseID)) {
            (*(stock.RemoteCount))++;
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
    float _w_tax;
    float _d_tax;
    float _c_discount;
    float _total_amount = 0.0f;
};
