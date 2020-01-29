//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <vector>

#include "schema.h"
#include "tpcc_rand.h"


struct WarehouseData {
    std::vector<Warehouse> warehouses;
    std::vector<Stock> stocks;
    std::vector<District> districts;
    std::vector<Customer> customers;
    std::vector<History> history;
    std::vector<Order> orders;
    std::vector<NewOrder> new_orders;
    std::vector<OrderLine> order_lines;
};

WarehouseData generateWarehouseData(uint32_t id_start, uint32_t id_end)
{
    WarehouseData data;
    RandomContext random(0);

    for (uint32_t i=id_start; i < id_end; ++i) {
        data.warehouses.emplace_back(random, i);
        
        for (uint32_t j=1; j<100001; ++j) {
            data.stocks.emplace_back(random, i, j);
        }

        for (uint16_t j=1; j<11; ++j) {
            data.districts.emplace_back(random, i, j);
            generateCustomerData(data, random, i, j);
            generateOrderData(data, random, i, j);
        }
    }

    return data;
}

void generateCustomerData(WarehouseData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
{
    for (int i=1; i<3001;, ++i) {
        data.customers.emplace_back(random, w_id, d_id, i);
        data.history.emplace_back(random, w_id, d_id, i);
    }
}

void generateOrderData(WarehouseData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
{
    std::deque<uint32_t> permutationQueue(3000);
    for (int i=0; i<3000; ++i) {
        permutationQueue[i] = i + 1;
    }

    for (int i=1; i<3001; ++i) {
        uint32_t permuationIdx = random.UniformRandom(0, permutationQueue.size()-1);
        uint32_t c_id = permuationQueue[permutationIdx];
        permutationQueue.erase(permutationQueue.begin()+permutationIdx);

        data.orders.emplace_back(random, w_id, d_id, c_id, i);
        Order& order = data.orders.back();
        
        for (int j=1; j<=order.OrderLineCount; ++j) {
            data.order_lines.emplace_back(random, order, j);
        }

        if (i >= 2101) {
            data.new_orders.emplace_back(order);
        }
    }
}
