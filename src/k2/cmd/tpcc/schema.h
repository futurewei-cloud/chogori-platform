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

#include <string>

#include <k2/common/Common.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>

#include "tpcc_rand.h"

#define CHECK_READ_STATUS(read_result) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2DEBUG("TPC-C failed to read rows: " << (read_result).status); \
            return make_exception_future(std::runtime_error(k2::String("TPC-C failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

k2::String WIDToString(uint32_t id) {
    char chars[8];
    // We need leading 0s for lexographic ordering
    // Apparently snprintf is faster than stringstream
    snprintf(chars, 8, "%04u", id);
    return k2::String(chars);
}

template<typename ValueType>
seastar::future<k2::WriteResult> writeRow(const ValueType& row, k2::K2TxnHandle& txn)
{
    k2::dto::Key key = {};
    key.partitionKey = row.getPartitionKey();
    key.rangeKey = row.getRowKey();

    return txn.write(std::move(key), "TPCC", row.data).then([] (k2::WriteResult&& result) {
        if (!result.status.is2xxOK()) {
            K2DEBUG("writeRow failed: " << result.status);
            return seastar::make_exception_future<k2::WriteResult>(std::runtime_error("writeRow failed!"));
        }

        return seastar::make_ready_future<k2::WriteResult>(std::move(result));
    });
}

struct Address {
    Address () = default;
    Address (RandomContext& random) {
        Street_1 = random.RandomString(10, 20);
        Street_2 = random.RandomString(10, 20);
        City = random.RandomString(10, 20);
        State = random.RandomString(2, 2);
        Zip = random.RandomZipString();
    }

    std::optional<k2::String> Street_1;
    std::optional<k2::String> Street_2;
    std::optional<k2::String> City;
    std::optional<k2::String> State;
    std::optional<k2::String> Zip;

    SKV_RECORD_FIELDS(Street_1, Street_2, City, State, Zip);
};

uint64_t getDate()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

class Warehouse {
public:
    Warehouse(RandomContext& random, uint32_t id) : WarehouseID(id) {
        Name = random.RandomString(6, 10);
        address = Address(random);
        Tax = random.UniformRandom(0, 2000) / 10000.0f;
        YTD = _districts_per_warehouse() * _customers_per_district() * 1000;
    }

    std::optional<uint32_t> WarehouseID;
    std::optional<float> Tax; // TODO Needs to be fixed point to be in spec
    std::optional<uint32_t> YTD; // "Fixed point", first two digits are cents
    std::optional<k2::String> Name;
    Address address;

    Warehouse(const Warehouse::Data& d, uint32_t id) : WarehouseID(id) {
        data = d;
    }

    SKV_RECORD_FIELDS(WarehouseID, Tax, YTD, Name, address);

private:
    k2::ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
    k2::ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class District {
public:
    District(RandomContext& random, uint32_t w_id, uint32_t id) : WarehouseID(w_id), DistrictID(id) {
        Name = random.RandomString(6, 10);        
        address = Address(random);
        Tax = random.UniformRandom(0, 2000) / 10000.0f;
        YTD = _customers_per_district() * 1000;
        NextOrderID = _customers_per_district()+1;  
    }

    std::optional<uint32_t> WarehouseID;
    std::optional<uint32_t> DistrictID;
    std::optional<float> Tax; // TODO Needs to be fixed point to be in spec
    std::optional<uint32_t> YTD; // "Fixed point", first two digits are cents
    std::optional<uint32_t> NextOrderID;
    std::optional<k2::String> Name;
    Address address;

   District(const District::Data& d, uint32_t w_id, uint32_t id) : WarehouseID(w_id), DistrictID(id) {
       data = d;
   }

   SKV_RECORD_FIELDS(WarehouseID, DistrictID, Tax, YTD, NextOrderID, Name, address);

private:
   k2::ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class Customer {
public:
    Customer(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id) :
            WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id) {
        random.RandomString(5, 5, data.LastName); // TODO needs to use special non-uniform function
        strcpy(data.MiddleName, "OE");
        random.RandomString(8, 16, data.FirstName);
        data.address = Address(random);
        random.RandomNumericString(16, 16, data.Phone);
        data.SinceDate = getDate();

        uint32_t creditRoll = random.UniformRandom(1, 10);
        if (creditRoll == 1) {
            strcpy(data.Credit, "BC");
        } else {
            strcpy(data.Credit, "GC");
        }

        data.CreditLimit = 50000.0f;
        data.Discount = random.UniformRandom(0, 5000) / 10000.0f;
        data.Balance = -1000;
        data.YTDPayment = 1000;
        data.PaymentCount = 1;
        data.DeliveryCount = 0;
        random.RandomString(300, 500, data.Info);
    }

    k2::String getPartitionKey() const { return WIDToString(WarehouseID); }
    k2::String getRowKey() const { return "CUST:" + std::to_string(DistrictID) + ":" + std::to_string(CustomerID); }
    static k2::dto::Key getKey(uint32_t w_id, uint16_t d_id, uint32_t c_id) {
        k2::dto::Key key = {
            .partitionKey = WIDToString(w_id),
            .rangeKey = "CUST:" + std::to_string(d_id) + ":" + std::to_string(c_id)
        };
        return key;
    }

    uint32_t WarehouseID;
    uint16_t DistrictID;
    uint32_t CustomerID;
    struct Data {
        uint64_t SinceDate;
        float CreditLimit; // TODO Needs to be fixed point to be in spec
        float Discount;
        int32_t Balance;
        uint32_t YTDPayment;
        uint16_t PaymentCount;
        uint16_t DeliveryCount;
        char FirstName[17];
        char MiddleName[3];
        char LastName[17];
        Address address;
        char Phone[17];
        char Credit[3]; // "GC" or "BC"
        char Info[501];
        K2_PAYLOAD_COPYABLE;
    } data;

    Customer(const Customer::Data& d, uint32_t w_id, uint16_t d_id, uint32_t c_id) : WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id) {
        data = d;
    }
};

class History {
public:
    // For initial population
    History(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id) : WarehouseID(w_id) {
        data.CustomerID = c_id;
        data.CustomerWarehouseID = w_id;
        data.CustomerDistrictID = d_id;
        data.Date = getDate();
        data.Amount = 1000;
        random.RandomString(12, 24, data.Info);
    }

    // For payment transaction
    History(uint32_t w_id, uint16_t d_id, uint32_t c_id, uint32_t c_w_id, uint16_t c_d_id, float amount,
                const char w_name[], const char d_name[]) : WarehouseID(w_id) {

        data.Date = getDate();
        data.CustomerID = c_id;
        data.CustomerWarehouseID = c_w_id;
        data.Amount = amount;
        data.CustomerDistrictID = c_d_id;
        data.DistrictID = d_id;

        strcpy(data.Info, w_name);
        uint32_t offset = strlen(w_name);
        const char separator[] = "    ";
        strcpy(data.Info+offset, separator);
        offset += strlen(separator);
        strcpy(data.Info+offset, d_name);
    }

    k2::String getPartitionKey() const { return WIDToString(WarehouseID); }
    k2::String getRowKey() const { return "HIST:" + std::to_string(data.Date); }

    uint32_t WarehouseID;
    struct Data {
        uint64_t Date;
        uint32_t CustomerID;
        uint32_t CustomerWarehouseID;
        uint32_t Amount;
        uint16_t CustomerDistrictID;
        uint16_t DistrictID;
        char Info[25];
        K2_PAYLOAD_COPYABLE;
    } data;
};

class Order {
public:
    // For initial population
    Order(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id, uint32_t id) :
            WarehouseID(w_id), OrderID(id), DistrictID(d_id) {
        data.CustomerID = c_id;
        data.EntryDate = 0; // TODO
        if (id < 2101) {
            data.CarrierID = random.UniformRandom(1, 10);
        } else {
            data.CarrierID = 0;
        }
        OrderLineCount = random.UniformRandom(5, 15);
        data.AllLocal = true;
    }

    // For NewOrder transaction
    Order(RandomContext& random, uint32_t w_id) : WarehouseID(w_id) {
        DistrictID = random.UniformRandom(1, _districts_per_warehouse());
        data.CustomerID = random.NonUniformRandom(1023, 1, _customers_per_district());
        OrderLineCount = random.UniformRandom(5, 15);
        data.EntryDate = getDate();
        data.CarrierID = 0;
        // OrderID and AllLocal to be filled in by the transaction
    }

    k2::String getPartitionKey() const { return WIDToString(WarehouseID); }
    k2::String getRowKey() const { return "ORDER:" + std::to_string(DistrictID) + ":" + std::to_string(OrderID); }
    static k2::dto::Key getKey(uint32_t w_id, uint16_t d_id, uint32_t o_id) {
        k2::dto::Key key = {
            .partitionKey = WIDToString(w_id),
            .rangeKey = "ORDER:" + std::to_string(d_id) + ":" + std::to_string(o_id)
        };
        return key;
    }


    uint32_t WarehouseID;
    uint32_t OrderID;
    uint16_t DistrictID;
    uint16_t OrderLineCount;

    struct Data {
        uint64_t EntryDate;
        uint32_t CustomerID;
        uint16_t CarrierID;
        bool AllLocal;
        K2_PAYLOAD_COPYABLE;
    } data;

private:
    k2::ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
    k2::ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class NewOrder {
public:
    NewOrder(const Order& order) : WarehouseID(order.WarehouseID), OrderID(order.OrderID), DistrictID(order.DistrictID) {}

    k2::String getPartitionKey() const { return WIDToString(WarehouseID); }
    k2::String getRowKey() const { return "NEW:" + std::to_string(DistrictID) + ":" + std::to_string(OrderID); }

    uint32_t WarehouseID;
    uint32_t OrderID;
    uint16_t DistrictID;
};

// NewOrder does not have data, so specialize and write an empty Payload
template<>
seastar::future<k2::WriteResult> writeRow<NewOrder>(const NewOrder& row, k2::K2TxnHandle& txn)
{
    k2::dto::Key key = {};
    key.partitionKey = row.getPartitionKey();
    key.rangeKey = row.getRowKey();

    return txn.write(std::move(key), "TPCC", 0).then([] (k2::WriteResult&& result) {
        if (!result.status.is2xxOK()) {
            K2DEBUG("writeRow failed: " << result.status);
            return seastar::make_exception_future<k2::WriteResult>(std::runtime_error("writeRow failed!"));
        }

        return seastar::make_ready_future<k2::WriteResult>(std::move(result));
    });
}


class OrderLine {
public:
    // For initial population
    OrderLine(RandomContext& random, const Order& order, uint16_t line_num) :
            WarehouseID(order.WarehouseID), OrderID(order.OrderID), DistrictID(order.DistrictID), OrderLineNumber(line_num) {
        data.ItemID = random.UniformRandom(1, 100000);
        data.SupplyWarehouseID = WarehouseID;

        if (order.OrderID < 2101) {
            data.DeliveryDate = order.data.EntryDate;
            data.Amount = 0.0f;
        } else {
            data.DeliveryDate = 0;
            data.Amount = random.UniformRandom(1, 999999) / 100.0f;
        }

        data.Quantity = 5;
        random.RandomString(24, 24, data.DistInfo);
    }

    // For New-Order transaction
    // Amount and DistInfo must be filled in during transaction
    // ItemID must be changed if it needs to be a rollback transactiom
    OrderLine(RandomContext& random, const Order& order, uint16_t line_num, uint32_t max_warehouse_id) :
            WarehouseID(order.WarehouseID), OrderID(order.OrderID), DistrictID(order.DistrictID), OrderLineNumber(line_num) {
        data.ItemID = random.NonUniformRandom(8191, 1, 100000);

        uint32_t homeRoll = random.UniformRandom(1, 100);
        if (homeRoll == 1 && max_warehouse_id > 1) {
            do {
                data.SupplyWarehouseID = random.UniformRandom(1, max_warehouse_id);
            } while (data.SupplyWarehouseID == WarehouseID);
        } else {
            data.SupplyWarehouseID = WarehouseID;
        }

        data.Quantity = random.UniformRandom(1, 10);
        data.Amount = 0.0f;
    }

    k2::String getPartitionKey() const { return WIDToString(WarehouseID); }
    k2::String getRowKey() const { return "ORDERLINE:" + std::to_string(DistrictID) + ":" + std::to_string(OrderID) + ":" + std::to_string(OrderLineNumber); }

    uint32_t WarehouseID;
    uint32_t OrderID;
    uint16_t DistrictID;
    uint16_t OrderLineNumber;

    struct Data {
        uint64_t DeliveryDate;
        uint32_t ItemID;
        uint32_t SupplyWarehouseID;
        float Amount; // TODO
        uint16_t Quantity;
        char DistInfo[25];
        K2_PAYLOAD_COPYABLE;
    } data;
};

class Item {
public:
    static const uint32_t InvalidID = 999999;

    Item(RandomContext& random, uint32_t id) : ItemID(id) {
        data.ImageID = random.UniformRandom(1, 10000);
        random.RandomString(14, 24, data.Name);
        data.Price = random.UniformRandom(100, 10000) / 100.0f;
        random.RandomString(26, 50, data.Info);
        uint32_t originalRoll = random.UniformRandom(1, 10);
        if (originalRoll == 1) {
            const char original[] = "ORIGINAL";
            uint32_t length = strlen(data.Info);
            uint32_t originalStart = random.UniformRandom(0, length-8);
            memcpy(data.Info+originalStart, original, 8);
        }
    }

    k2::String getPartitionKey() const { return "ITEM:" + std::to_string(ItemID); }
    k2::String getRowKey() const { return ""; }
    static k2::dto::Key getKey(uint32_t id) {
        k2::dto::Key key = {
            .partitionKey = "ITEM:" + std::to_string(id),
            .rangeKey = ""
        };
        return key;
    }

    uint32_t ItemID;

    struct Data {
        uint32_t ImageID;
        float Price; // TODO
        char Name[25];
        char Info[51];
        K2_PAYLOAD_COPYABLE;
    } data;

    Item(const Item::Data& d, uint32_t id) : ItemID(id) {
        data = d;
    }
};

class Stock {
public:
    Stock(RandomContext& random, uint32_t w_id, uint32_t i_id) : WarehouseID(w_id), ItemID(i_id) {
        data.Quantity = random.UniformRandom(10, 100);
        random.RandomString(24, 24, data.Dist_01);
        random.RandomString(24, 24, data.Dist_02);
        random.RandomString(24, 24, data.Dist_03);
        random.RandomString(24, 24, data.Dist_04);
        random.RandomString(24, 24, data.Dist_05);
        random.RandomString(24, 24, data.Dist_06);
        random.RandomString(24, 24, data.Dist_07);
        random.RandomString(24, 24, data.Dist_08);
        random.RandomString(24, 24, data.Dist_09);
        random.RandomString(24, 24, data.Dist_10);
        data.YTD = 0.0f;
        data.OrderCount = 0;
        data.RemoteCount = 0;
        random.RandomString(26, 50, data.Info);
        uint32_t originalRoll = random.UniformRandom(1, 10);
        if (originalRoll == 1) {
            const char original[] = "ORIGINAL";
            uint32_t length = strlen(data.Info);
            uint32_t originalStart = random.UniformRandom(0, length-8);
            memcpy(data.Info+originalStart, original, 8);
        }
    }

    k2::String getPartitionKey() const { return WIDToString(WarehouseID); }
    k2::String getRowKey() const { return "STOCK:" + std::to_string(ItemID); }
    static k2::dto::Key getKey(uint32_t w_id, uint32_t i_id) {
        k2::dto::Key key = {
            .partitionKey = WIDToString(w_id),
            .rangeKey = "STOCK:" + std::to_string(i_id)
        };
        return key;
    }

    const char* getDistInfo(uint16_t d_id) {
        switch(d_id) {
            case 1:
                return data.Dist_01;
            case 2:
                return data.Dist_02;
            case 3:
                return data.Dist_03;
            case 4:
                return data.Dist_04;
            case 5:
                return data.Dist_05;
            case 6:
                return data.Dist_06;
            case 7:
                return data.Dist_07;
            case 8:
                return data.Dist_08;
            case 9:
                return data.Dist_09;
            case 10:
                return data.Dist_10;
            default:
                throw 0;
        }
    }

    uint32_t WarehouseID;
    uint32_t ItemID;

    struct Data {
        float YTD; // TODO
        uint16_t OrderCount;
        uint16_t RemoteCount;
        uint16_t Quantity;
        char Dist_01[25];
        char Dist_02[25];
        char Dist_03[25];
        char Dist_04[25];
        char Dist_05[25];
        char Dist_06[25];
        char Dist_07[25];
        char Dist_08[25];
        char Dist_09[25];
        char Dist_10[25];
        char Info[51];
        K2_PAYLOAD_COPYABLE;
    } data;

    Stock(const Stock::Data& d, uint32_t w_id, uint32_t i_id) : WarehouseID(w_id), ItemID(i_id) {
        data = d;
    }
};
