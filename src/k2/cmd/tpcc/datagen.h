//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <seastar/core/future.hh>
#include <k2/module/k23si/client/k23si_client.h>

#include <vector>

#include "schema.h"
#include "tpcc_rand.h"

typedef std::vector<std::function<seastar::future<k2::WriteResult>(k2::K2TxnHandle&)>> TPCCData;

TPCCData generateItemData()
{
    TPCCData data;
    data.reserve(100000);
    RandomContext random(0);

    for (int i=1; i<=100000; ++i) {
        auto item = Item(random, i);
        data.push_back([_item=std::move(item)] (k2::K2TxnHandle& txn) {
            return writeRow<Item>(std::move(_item), txn);
        });
    }

    return data;
}

void generateCustomerData(TPCCData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
{
    for (int i=1; i<3001; ++i) {
        auto customer = Customer(random, w_id, d_id, i);
        data.push_back([_customer=std::move(customer)] (k2::K2TxnHandle& txn) {
            return writeRow<Customer>(std::move(_customer), txn);
        });

        auto history = History(random, w_id, d_id, i);
        data.push_back([_history=std::move(history)] (k2::K2TxnHandle& txn) {
            return writeRow<History>(std::move(_history), txn);
        });
    }
}

void generateOrderData(TPCCData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
{
    std::deque<uint32_t> permutationQueue(3000);
    for (int i=0; i<3000; ++i) {
        permutationQueue[i] = i + 1;
    }

    for (int i=1; i<3001; ++i) {
        uint32_t permutationIdx = random.UniformRandom(0, permutationQueue.size()-1);
        uint32_t c_id = permutationQueue[permutationIdx];
        permutationQueue.erase(permutationQueue.begin()+permutationIdx);

        auto order = Order(random, w_id, d_id, c_id, i);
        
        for (int j=1; j<=order.OrderLineCount; ++j) {
            auto order_line = OrderLine(random, order, j);
            data.push_back([_order_line=std::move(order_line)] (k2::K2TxnHandle& txn) {
                return writeRow<OrderLine>(std::move(_order_line), txn);
            });
        }

        if (i >= 2101) {
            auto new_order = NewOrder(order);
            data.push_back([_new_order=std::move(new_order)] (k2::K2TxnHandle& txn) {
                return writeRow<NewOrder>(std::move(_new_order), txn);
            });
        }

        data.push_back([_order=std::move(order)] (k2::K2TxnHandle& txn) {
            return writeRow<Order>(std::move(_order), txn);
        });
    }
}

TPCCData generateWarehouseData(uint32_t id_start, uint32_t id_end)
{
    TPCCData data;

    uint32_t num_warehouses = id_end - id_start;
    size_t reserve_space = 0;
    reserve_space += num_warehouses;
    reserve_space += num_warehouses*10000;
    reserve_space += num_warehouses*10;
    reserve_space += num_warehouses*10*3000;
    reserve_space += num_warehouses*10*3000;
    reserve_space += num_warehouses*10*3000;
    reserve_space += num_warehouses*10*3000*10;
    reserve_space += num_warehouses*10*900;
    data.reserve(reserve_space);
    RandomContext random(0);

    for (uint32_t i=id_start; i < id_end; ++i) {
        auto warehouse = Warehouse(random, i);

        data.push_back([_warehouse=std::move(warehouse)] (k2::K2TxnHandle& txn) {
            return writeRow<Warehouse>(std::move(_warehouse), txn);
        });
        
        for (uint32_t j=1; j<100001; ++j) {
            auto stock = Stock(random, i, j);
            data.push_back([_stock=std::move(stock)] (k2::K2TxnHandle& txn) {
                return writeRow<Stock>(std::move(_stock), txn);
            });
        }

        for (uint16_t j=1; j<11; ++j) {
            auto district = District(random, i, j);
            data.push_back([_district=std::move(district)] (k2::K2TxnHandle& txn) {
                return writeRow<District>(std::move(_district), txn);
            });

            generateCustomerData(data, random, i, j);
            generateOrderData(data, random, i, j);
        }
    }

    return data;
}
