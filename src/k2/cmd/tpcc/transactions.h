//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <utility>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/RetryStrategy.h>
#include <seastar/core/sleep.hh>

#include "mock/mock_k23si_client.h"
#include "schema.h"

using namespace seastar;
using namespace k2;

class PaymentT
{
public:
    PaymentT(RandomContext& random, K23SIClient& client, uint32_t w_id, uint32_t max_w_id) :
                        _client(client), _w_id(w_id) {

        _d_id = random.UniformRandom(1, 10);
        _c_id = random.NonUniformRandom(1023, 1, 3000); // TODO, by last name
        uint32_t local = random.UniformRandom(1, 100);
        if (local <= 85) {
            _c_w_id = _w_id;
            _c_d_id = _d_id;
        } else {
            do {
                _c_w_id = random.UniformRandom(1, max_w_id);
            } while (_c_w_id == _w_id);
            _c_d_id = random.UniformRandom(1, 10);
        }

        _amount = random.UniformRandom(100, 500000) / 100.0f;
    }

    future<> run() {
        K2TxnOptions options;
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle txn) {
            _txn = std::move(txn);
            return runWithTxn();
        });
    }

private:
    future<> runWithTxn() {
        future<> warehouse_update = warehouseUpdate();
        future<> district_update = districtUpdate();
        future<> customer_update = customerUpdate();

        future<> history_update = when_all(std::move(warehouse_update), std::move(district_update))
        .then([this] (auto&& results) {
            (void) results;
            return historyUpdate();
        });

        return when_all(std::move(customer_update), std::move(history_update))
        .then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                return _txn.end(false);
            }

            K2INFO("Payment txn finished");

            return _txn.end(true);         
        }).discard_result();
    }

    future<> warehouseUpdate() {
        return _txn.read(Warehouse::getKey(_w_id))
        .then([this] (ReadResult result) {
            CHECK_READ_STATUS(result);
            Warehouse warehouse(result, _w_id);
            warehouse.data.YTD += _amount;
            strcpy(_w_name, warehouse.data.Name);
            return writeRow(warehouse, _txn).discard_result();
        });
    }

    future<> districtUpdate() {
        return _txn.read(District::getKey(_w_id, _d_id))
        .then([this] (ReadResult result) {
            CHECK_READ_STATUS(result);
            District district(result, _w_id, _d_id);
            district.data.YTD += _amount;
            strcpy(_d_name, district.data.Name);
            return writeRow(district, _txn).discard_result();
        });
    }

    future<> customerUpdate() {
        return _txn.read(Customer::getKey(_c_w_id, _c_d_id, _c_id))
        .then([this] (ReadResult result) {
            CHECK_READ_STATUS(result);
            Customer customer(result, _c_w_id, _c_d_id, _c_id);

            customer.data.Balance -= _amount;
            customer.data.YTDPayment += _amount;
            customer.data.PaymentCount++;

            if (strcmp(customer.data.Credit, "BC") == 0) {
                size_t shift_size = sizeof(_c_id) + sizeof(_c_d_id) + sizeof(_d_id) + sizeof(_w_id) + sizeof(_amount);
                memmove(customer.data.Info + shift_size, customer.data.Info, 500-shift_size);
                customer.data.Info[500] = '\0';
                uint32_t offset = 0;
                memcpy(customer.data.Info+offset, &_c_id, sizeof(_c_id));
                offset += sizeof(_c_id);
                memcpy(customer.data.Info+offset, &_c_d_id, sizeof(_c_d_id));
                offset += sizeof(_c_d_id);
                memcpy(customer.data.Info+offset, &_d_id, sizeof(_d_id));
                offset += sizeof(_d_id);
                memcpy(customer.data.Info+offset, &_w_id, sizeof(_w_id));
                offset += sizeof(_w_id);
                memcpy(customer.data.Info+offset, &_amount, sizeof(_amount));
            }

            return writeRow(customer, _txn).discard_result();
        });
    }

    future<> historyUpdate() {
        History history(_w_id, _d_id, _c_id, _c_w_id, _c_d_id, _amount, _w_name, _d_name);
        return writeRow(history, _txn).discard_result();
    }

    K23SIClient& _client;
    K2TxnHandle _txn;
    uint32_t _w_id;
    uint16_t _d_id;
    uint32_t _c_id;
    uint32_t _c_w_id;
    uint16_t _c_d_id;
    float _amount;
    char _w_name[11];
    char _d_name[11];
};

class NewOrderT
{
public:
    NewOrderT(RandomContext& random, K23SIClient& client, uint32_t w_id, uint32_t max_w_id) : 
                        _random(random), _client(client), _w_id(w_id), _max_w_id(max_w_id), _order(random, w_id) {}

    future<> run() {
        K2TxnOptions options;
        return _client.beginTxn(options)
        .then([this] (K2TxnHandle txn) {
            _txn = std::move(txn);
            return runWithTxn();
        });
    }

private:
    future<> runWithTxn() {
        // Get warehouse row, only used for tax rate in total amount calculation
        future<> warehouse_f = _txn.read(Warehouse::getKey(_w_id))
        .then([this] (ReadResult result) {
            CHECK_READ_STATUS(result);
            Warehouse warehouse(result, _w_id);
            _w_tax = warehouse.data.Tax;
            return make_ready_future();
        });

        // Get customer row, only used for discount rate in total amount calculation
        future<> customer_f = _txn.read(Customer::getKey(_w_id, _order.DistrictID, _order.data.CustomerID))
        .then([this] (ReadResult result) {
            CHECK_READ_STATUS(result);
            Customer customer(result, _w_id, _order.DistrictID, _order.data.CustomerID);
            _c_discount = customer.data.Discount;
            return make_ready_future();
        });

         future<> main_f = _txn.read(District::getKey(_w_id, _order.DistrictID))
        .then([this] (ReadResult result) {
            CHECK_READ_STATUS(result);

            // Get and write NextOrderID in district row
            District district(result, _w_id, _order.DistrictID);
            _order.OrderID = district.data.NextOrderID;
            _d_tax = district.data.Tax;
            district.data.NextOrderID++;
            future<WriteResult> district_update = writeRow(district, _txn);

            // Write NewOrder row
            NewOrder new_order(_order);
            future<WriteResult> new_order_update = writeRow(new_order, _txn);

            makeOrderLines();

            // Write Order row
            future<WriteResult> order_update = writeRow(_order, _txn);

            future<> line_updates = parallel_for_each(_lines.begin(), _lines.end(), [this] (OrderLine& line) {
                return _txn.read(Item::getKey(line.data.ItemID))
                .then([this, i_id=line.data.ItemID] (ReadResult result) {
                    if (!result.status.is2xxOK()) {
                        return _txn.end(false).then([] (EndResult result) { (void) result; return make_exception_future<Item>(std::runtime_error("Bad ItemID")); });
                    }

                    return make_ready_future<Item>(Item(result, i_id));

                }).then([this, supply_id=line.data.SupplyWarehouseID] (Item item) {
                    return _txn.read(Stock::getKey(supply_id, item.ItemID))
                    .then([item, supply_id] (ReadResult result) {
                        return make_ready_future<std::pair<Item, Stock>>(std::make_pair(std::move(item), Stock(result, supply_id, item.ItemID)));
                    });

                }).then([this, &line] (std::pair<Item, Stock> pair) {
                    auto& [item, stock] = pair;
                    line.data.Amount = item.data.Price * line.data.Quantity;
                    _total_amount += line.data.Amount;
                    strcpy(line.data.DistInfo, stock.getDistInfo(line.DistrictID));
                    auto line_update = writeRow(line, _txn);

                    updateStockRow(stock, line);
                    auto stock_update = writeRow(stock, _txn);

                    return when_all(std::move(line_update), std::move(stock_update)).discard_result();
                });
            });

            return when_all(std::move(line_updates), std::move(order_update), std::move(new_order_update), std::move(district_update)).discard_result();
        });

        return when_all(std::move(main_f), std::move(customer_f), std::move(warehouse_f))
        .then_wrapped([this] (auto&& fut) {
            if (fut.failed()) {
                return _txn.end(false);
            }

            _total_amount *= (1 - _c_discount) * (1 + _w_tax + _d_tax);
            K2INFO("NewOrder _total_amount: " << _total_amount);

            return _txn.end(true);
        }).discard_result();
    }

    static void updateStockRow(Stock& stock, const OrderLine& line) {
        if (stock.data.Quantity - line.data.Quantity >= 10) {
            stock.data.Quantity -= line.data.Quantity;
        } else {
            stock.data.Quantity = stock.data.Quantity + 91 - line.data.Quantity;
        }
        stock.data.YTD += line.data.Quantity;
        stock.data.OrderCount++;
        if (line.WarehouseID != line.data.SupplyWarehouseID) {
            stock.data.RemoteCount++;
        }
    }

    void makeOrderLines() {
        _lines.reserve(_order.OrderLineCount);
        _order.data.AllLocal = true;
        for (int i=0; i<_order.OrderLineCount; ++i) {
            _lines.emplace_back(_random, _order, i, _max_w_id);
            if (_lines.back().data.SupplyWarehouseID != _w_id) {
                _order.data.AllLocal = false;
            }
        }
        uint32_t rollback = _random.UniformRandom(1, 100);
        if (rollback == 1) {
            _lines.back().data.ItemID = Item::InvalidID;
        }
    }

    RandomContext& _random;
    K23SIClient& _client;
    K2TxnHandle _txn;
    uint32_t _w_id;
    uint32_t _max_w_id;
    Order _order;
    std::vector<OrderLine> _lines;
    // The below variables are needed to "display" the order total amount,
    // but are not needed for any DB operations
    float _w_tax;
    float _d_tax;
    float _c_discount;
    float _total_amount = 0.0f;
};
