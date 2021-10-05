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

#include <seastar/core/future.hh>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/config/Config.h>

#include <vector>

#include "schema.h"
#include "tpcc_rand.h"

typedef std::vector<std::function<seastar::future<k2::WriteResult>(k2::K2TxnHandle&)>> TPCCData;

struct TPCCDataGen {
    TPCCData generateItemData()
    {
        TPCCData data;
        data.reserve(100000);
        RandomContext random(0);

        for (int i=1; i<=100000; ++i) {
            auto item = Item(random, i);
            data.push_back([_item=std::move(item)] (k2::K2TxnHandle& txn) mutable {
                return writeRow<Item>(_item, txn);
            });
        }

        return data;
    }

    void generateCustomerData(TPCCData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
    {
        for (uint16_t i=1; i <= _customers_per_district(); ++i) {
            auto customer = Customer(random, w_id, d_id, i);

            // populate secondary index idx_customer_name
            auto idx_customer_name = IdxCustomerName(customer.WarehouseID.value(), customer.DistrictID.value(),
                customer.LastName.value(), customer.CustomerID.value());
            data.push_back([_idx_customer_name=std::move(idx_customer_name)] (k2::K2TxnHandle& txn) mutable {
                return writeRow<IdxCustomerName>(_idx_customer_name, txn);
            });

            data.push_back([_customer=std::move(customer)] (k2::K2TxnHandle& txn) mutable {
                return writeRow<Customer>(_customer, txn);
            });

            auto history = History(random, w_id, d_id, i);
            data.push_back([_history=std::move(history)] (k2::K2TxnHandle& txn) mutable {
                return writeRow<History>(_history, txn);
            });
        }
    }

    void generateOrderData(TPCCData& data, RandomContext& random, uint32_t w_id, uint16_t d_id)
    {
        std::deque<uint32_t> permutationQueue(_customers_per_district());
        for (uint16_t i=0; i< _customers_per_district(); ++i) {
            permutationQueue[i] = i + 1;
        }

        for (uint16_t i=1; i <= _customers_per_district(); ++i) {
            uint32_t permutationIdx = random.UniformRandom(0, permutationQueue.size()-1);
            uint32_t c_id = permutationQueue[permutationIdx];
            permutationQueue.erase(permutationQueue.begin()+permutationIdx);

            auto order = Order(random, w_id, d_id, c_id, i);

            for (int j=1; j<=order.OrderLineCount; ++j) {
                auto order_line = OrderLine(random, order, j);
                data.push_back([_order_line=std::move(order_line)] (k2::K2TxnHandle& txn) mutable {
                    return writeRow<OrderLine>(_order_line, txn);
                });
            }

            if (i >= 2101) {
                auto new_order = NewOrder(order);
                data.push_back([_new_order=std::move(new_order)] (k2::K2TxnHandle& txn) mutable {
                    return writeRow<NewOrder>(_new_order, txn);
                });
            }

            auto idx_order_customer = IdxOrderCustomer(order.WarehouseID.value(), order.DistrictID.value(),
                                     order.CustomerID.value(), order.OrderID.value());

            data.push_back([_order=std::move(order)] (k2::K2TxnHandle& txn) mutable {
                return writeRow<Order>(_order, txn);
            });

            // populate secondary index idx_order_customer
            data.push_back([_idx_order_customer=std::move(idx_order_customer)] (k2::K2TxnHandle& txn) mutable {
                return writeRow<IdxOrderCustomer>(_idx_order_customer, txn);
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
        RandomContext random(id_start);

        for (uint32_t i=id_start; i < id_end; ++i) {
            auto warehouse = Warehouse(random, i);

            data.push_back([_warehouse=std::move(warehouse)] (k2::K2TxnHandle& txn) mutable {
                return writeRow<Warehouse>(_warehouse, txn);
            });

            for (uint32_t j=1; j<100001; ++j) {
                auto stock = Stock(random, i, j);
                data.push_back([_stock=std::move(stock)] (k2::K2TxnHandle& txn) mutable {
                    return writeRow<Stock>(_stock, txn);
                });
            }

            for (uint16_t j=1; j <= _districts_per_warehouse(); ++j) {
                auto district = District(random, i, j);
                data.push_back([_district=std::move(district)] (k2::K2TxnHandle& txn) mutable {
                    return writeRow<District>(_district, txn);
                });

                generateCustomerData(data, random, i, j);
                generateOrderData(data, random, i, j);
            }
        }

        return data;
    }

private:
    k2::ConfigVar<int16_t> _districts_per_warehouse{"districts_per_warehouse"};
    k2::ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};
