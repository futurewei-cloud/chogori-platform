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

future<> NewOrderT(RandomContext& random, K2TxnHandle& txn, uint32_t w_id, uint32_t max_w_id)
{
    Order order(random, w_id);

    return when_all(txn.read(Warehouse::getKey(w_id)), 
                            txn.read(District::getKey(w_id, order.DistrictID)),
                            txn.read(Customer::getKey(w_id, order.DistrictID, order.data.CustomerID)))
    .then([&random, &txn, order, w_id, max_w_id] (auto tuple) mutable {
        auto [w, d, c] = std::move(tuple);
        if (!w.get0().status.is2xxOK() || !d.get0().status.is2xxOK() || !c.get0().status.is2xxOK()) {
            K2WARN("TPC-C failed to read rows!");
            return make_exception_future(std::runtime_error("TPC-C failed to read rows!"));
        }


        Warehouse warehouse(w.get0(), w_id);
        District district(d.get0(), w_id, order.DistrictID);
        Customer customer(c.get0(), w_id, order.DistrictID, order.data.CustomerID);

        order.OrderID = district.data.NextOrderID;
        district.data.NextOrderID++;
        future<WriteResult> district_update = writeRow(district, txn);

        NewOrder new_order(order);
        future<WriteResult> new_order_update = writeRow(new_order, txn);

        std::vector<OrderLine> lines;
        lines.reserve(order.OrderLineCount);
        order.data.AllLocal = true;
        for (int i=0; i<order.OrderLineCount; ++i) {
            lines.emplace_back(random, order, i, max_w_id);
            if (lines.back().data.SupplyWarehouseID != w_id) {
                order.data.AllLocal = false;
            }
        }
        uint32_t rollback = random.UniformRandom(1, 100);
        if (rollback == 1) {
            lines.back().data.ItemID = Item::InvalidID;
        }

        future<WriteResult> order_update = writeRow(order, txn);

        future<> line_updates = do_with(std::move(lines), [&txn] (std::vector<OrderLine>& l) {
            return parallel_for_each(l.begin(), l.end(), [&txn] (OrderLine& line) {

                return txn.read(Item::getKey(line.data.ItemID))
                .then([&txn, i_id=line.data.ItemID] (ReadResult result) {
                    if (!result.status.is2xxOK()) {
                        return make_exception_future<Item>(std::runtime_error("Bad ItemID"));
                    }

                    return make_ready_future<Item>(Item(result, i_id));

                }).then([&txn, supply_id=line.data.SupplyWarehouseID] (Item item) {
                    return txn.read(Stock::getKey(supply_id, item.ItemID))
                    .then([item, supply_id] (ReadResult result) {
                        return make_ready_future<std::pair<Item, Stock>>(std::make_pair(item, Stock(result, supply_id, item.ItemID)));
                    });

                }).then([&txn, &line] (std::pair<Item, Stock> pair) {
                    auto& [item, stock] = pair;
                    line.data.Amount = item.data.Price * line.data.Quantity;
                    strcpy(line.data.DistInfo, stock.getDistInfo(line.DistrictID));
                    auto line_update = writeRow(line, txn);

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

                    auto stock_update = writeRow(stock, txn);

                    return when_all(std::move(line_update), std::move(stock_update)).discard_result();
                });
            });
        });

        return when_all(std::move(line_updates), std::move(order_update), std::move(new_order_update), std::move(district_update)).discard_result();
    })
    .then_wrapped([&txn] (auto&& fut) {
        if (fut.failed()) {
            return txn.end(false);
        }

        return txn.end(true);
    }).discard_result();
}

