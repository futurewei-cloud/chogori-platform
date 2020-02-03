//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <vector>

#include "schema.h"
#include "tpcc_rand.h"

typedef std::vector<std::unique_ptr<SchemaType>> TPCCData;

TPCCData generateItemData()
{
    TPCCData data;
    data.reserve(100000);
    RandomContext random(0);

    for (int i=1; i<=100000; ++i) {
        data.push_back(std::make_unique<Item>(random, i));
    }

    return data;
}

void generateCustomerData(TPCCData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
{
    for (int i=1; i<3001; ++i) {
        data.push_back(std::make_unique<Customer>(random, w_id, d_id, i));
        data.push_back(std::make_unique<History>(random, w_id, d_id, i));
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

        data.push_back(std::make_unique<Order>(random, w_id, d_id, c_id, i));
        Order& order = dynamic_cast<Order&>(*data.back());
        
        for (int j=1; j<=order.OrderLineCount; ++j) {
            data.push_back(std::make_unique<OrderLine>(random, order, j));
        }

        if (i >= 2101) {
            data.push_back(std::make_unique<NewOrder>(order));
        }
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
        data.push_back(std::make_unique<Warehouse>(random, i));
        
        for (uint32_t j=1; j<100001; ++j) {
            data.push_back(std::make_unique<Stock>(random, i, j));
        }

        for (uint16_t j=1; j<11; ++j) {
            data.push_back(std::make_unique<District>(random, i, j));
            generateCustomerData(data, random, i, j);
            generateOrderData(data, random, i, j);
        }
    }

    return data;
}
