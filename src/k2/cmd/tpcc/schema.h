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

static const k2::String tpccCollectionName = "TPCC";

#define CHECK_READ_STATUS(read_result) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2DEBUG("TPC-C failed to read rows: " << (read_result).status); \
            return make_exception_future(std::runtime_error(k2::String("TPC-C failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

template<typename ValueType>
seastar::future<k2::WriteResult> writeRow(ValueType& row, k2::K2TxnHandle& txn)
{
    return txn.write<ValueType>(row).then([] (k2::WriteResult&& result) {
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
    static inline k2::dto::Schema warehouse_schema {
		.name = "warehouse",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::FLOAT, "Tax", false, false},
				{k2::dto::FieldType::UINT32T, "YTD", false, false},
				{k2::dto::FieldType::STRING, "Name", false, false},
				{k2::dto::FieldType::STRING, "Street1", false, false},
				{k2::dto::FieldType::STRING, "Street2", false, false},
				{k2::dto::FieldType::STRING, "City", false, false},
				{k2::dto::FieldType::STRING, "State", false, false},
				{k2::dto::FieldType::STRING, "Zip", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };


    Warehouse(RandomContext& random, uint32_t id) : WarehouseID(id) {
        Name = random.RandomString(6, 10);
        address = Address(random);
        Tax = random.UniformRandom(0, 2000) / 10000.0f;
        YTD = _districts_per_warehouse() * _customers_per_district() * 1000;
    }

    Warehouse(uint32_t id) : WarehouseID(id) {}

    Warehouse() = default;

    std::optional<uint32_t> WarehouseID;
    std::optional<float> Tax; // TODO Needs to be fixed point to be in spec
    std::optional<uint32_t> YTD; // "Fixed point", first two digits are cents
    std::optional<k2::String> Name;
    Address address;

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;

    SKV_RECORD_FIELDS(WarehouseID, Tax, YTD, Name, address);

private:
    k2::ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
    k2::ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class District {
public:
    static inline k2::dto::Schema district_schema {
		.name = "district",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT32T, "DID", false, false},
				{k2::dto::FieldType::FLOAT, "Tax", false, false},
				{k2::dto::FieldType::UINT32T, "YTD", false, false},
				{k2::dto::FieldType::UINT32T, "NextOID", false, false},
				{k2::dto::FieldType::STRING, "Name", false, false},
				{k2::dto::FieldType::STRING, "Street1", false, false},
				{k2::dto::FieldType::STRING, "Street2", false, false},
				{k2::dto::FieldType::STRING, "City", false, false},
				{k2::dto::FieldType::STRING, "State", false, false},
				{k2::dto::FieldType::STRING, "Zip", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    District(RandomContext& random, uint32_t w_id, uint32_t id) : WarehouseID(w_id), DistrictID(id) {
        Name = random.RandomString(6, 10);        
        address = Address(random);
        Tax = random.UniformRandom(0, 2000) / 10000.0f;
        YTD = _customers_per_district() * 1000;
        NextOrderID = _customers_per_district()+1;  
    }

    District(uint32_t w_id, uint32_t id) : WarehouseID(w_id), DistrictID(id) {}

    District() = default;

    std::optional<uint32_t> WarehouseID;
    std::optional<uint32_t> DistrictID;
    std::optional<float> Tax; // TODO Needs to be fixed point to be in spec
    std::optional<uint32_t> YTD; // "Fixed point", first two digits are cents
    std::optional<uint32_t> NextOrderID;
    std::optional<k2::String> Name;
    Address address;

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;

    SKV_RECORD_FIELDS(WarehouseID, DistrictID, Tax, YTD, NextOrderID, Name, address);

private:
   k2::ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class Customer {
public:
    static inline k2::dto::Schema customer_schema {
		.name = "customer",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT32T, "DID", false, false},
				{k2::dto::FieldType::UINT32T, "CID", false, false},
				{k2::dto::FieldType::UINT64T, "SinceDate", false, false},
				{k2::dto::FieldType::FLOAT, "CreditLimit", false, false},
				{k2::dto::FieldType::FLOAT, "Discount", false, false},
				{k2::dto::FieldType::INT32T, "Balance", false, false},
				{k2::dto::FieldType::UINT32T, "YTDPayment", false, false},
				{k2::dto::FieldType::UINT32T, "PaymentCount", false, false},
				{k2::dto::FieldType::UINT32T, "DeliveryCount", false, false},
				{k2::dto::FieldType::STRING, "FirstName", false, false},
				{k2::dto::FieldType::STRING, "MiddleName", false, false},
				{k2::dto::FieldType::STRING, "LastName", false, false},
				{k2::dto::FieldType::STRING, "Phone", false, false},
				{k2::dto::FieldType::STRING, "Credit", false, false},
				{k2::dto::FieldType::STRING, "Info", false, false},
				{k2::dto::FieldType::STRING, "Street1", false, false},
				{k2::dto::FieldType::STRING, "Street2", false, false},
				{k2::dto::FieldType::STRING, "City", false, false},
				{k2::dto::FieldType::STRING, "State", false, false},
				{k2::dto::FieldType::STRING, "Zip", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2 }
    };

    Customer(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id) :
            WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id) {
        LastName = random.RandomString(5, 5); // TODO needs to use special non-uniform function
        MiddleName = "OE";
        FirstName = random.RandomString(8, 16);
        address = Address(random);
        Phone = random.RandomNumericString(16, 16);
        SinceDate = getDate();

        uint32_t creditRoll = random.UniformRandom(1, 10);
        if (creditRoll == 1) {
            Credit = "BC";
        } else {
            Credit = "GC";
        }

        CreditLimit = 50000.0f;
        Discount = random.UniformRandom(0, 5000) / 10000.0f;
        Balance = -1000;
        YTDPayment = 1000;
        PaymentCount = 1;
        DeliveryCount = 0;
        Info = random.RandomString(500, 500);
    }

    Customer(uint32_t w_id, uint16_t d_id, uint32_t c_id) :
            WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id) {}

    Customer() = default;

    std::optional<uint32_t> WarehouseID;
    std::optional<uint32_t> DistrictID;
    std::optional<uint32_t> CustomerID;
    std::optional<uint64_t> SinceDate;
    std::optional<float> CreditLimit; // TODO Needs to be fixed point to be in spec
    std::optional<float> Discount;
    std::optional<int32_t> Balance;
    std::optional<uint32_t> YTDPayment;
    std::optional<uint32_t> PaymentCount;
    std::optional<uint32_t> DeliveryCount;
    std::optional<k2::String> FirstName;
    std::optional<k2::String> MiddleName;
    std::optional<k2::String> LastName;
    std::optional<k2::String> Phone;
    std::optional<k2::String> Credit; // "GC" or "BC"
    std::optional<k2::String> Info;
    Address address;

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, CustomerID, SinceDate, CreditLimit, Discount, Balance,
        YTDPayment, PaymentCount, DeliveryCount, FirstName, MiddleName, LastName, Phone, Credit, 
        Info, address);
};

class History {
public:
    static inline k2::dto::Schema history_schema {
		.name = "history",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT64T, "Date", false, false},
				{k2::dto::FieldType::UINT32T, "CID", false, false},
				{k2::dto::FieldType::UINT32T, "CWID", false, false},
				{k2::dto::FieldType::UINT32T, "Amount", false, false},
				{k2::dto::FieldType::UINT32T, "CDID", false, false},
				{k2::dto::FieldType::UINT32T, "DID", false, false},
				{k2::dto::FieldType::STRING, "Info", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    // For initial population
    History(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id) : WarehouseID(w_id) {
        CustomerID = c_id;
        CustomerWarehouseID = w_id;
        CustomerDistrictID = d_id;
        Date = getDate();
        Amount = 1000;
        Info = random.RandomString(12, 24);
    }

    // For payment transaction
    History(uint32_t w_id, uint16_t d_id, uint32_t c_id, uint32_t c_w_id, uint16_t c_d_id, float amount,
                const char w_name[], const char d_name[]) : WarehouseID(w_id) {
        Date = getDate();
        CustomerID = c_id;
        CustomerWarehouseID = c_w_id;
        Amount = amount;
        CustomerDistrictID = c_d_id;
        DistrictID = d_id;

        Info = w_name;
        uint32_t offset = strlen(w_name);
        const char separator[] = "    ";
        strcpy((char*)Info->c_str() + offset, separator);
        offset += strlen(separator);
        strcpy((char*)Info->c_str() + offset, d_name);
    }

    History() = default;

    std::optional<uint32_t> WarehouseID;
    std::optional<uint64_t> Date;
    std::optional<uint32_t> CustomerID;
    std::optional<uint32_t> CustomerWarehouseID;
    std::optional<uint32_t> Amount;
    std::optional<uint32_t> CustomerDistrictID;
    std::optional<uint32_t> DistrictID;
    std::optional<k2::String> Info;

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, Date, CustomerID, CustomerWarehouseID, Amount, CustomerDistrictID,
        DistrictID, Info);
};

class Order {
public:
    static inline k2::dto::Schema order_schema {
		.name = "order",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT32T, "DID", false, false},
				{k2::dto::FieldType::UINT32T, "OID", false, false},
				{k2::dto::FieldType::UINT32T, "LineCount", false, false},
				{k2::dto::FieldType::UINT64T, "EntryDate", false, false},
				{k2::dto::FieldType::UINT32T, "CID", false, false},
				{k2::dto::FieldType::UINT32T, "CarrierID", false, false},
				{k2::dto::FieldType::UINT32T, "AllLocal", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2 }
    };

    // For initial population
    Order(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id, uint32_t id) :
            WarehouseID(w_id), DistrictID(d_id), OrderID(id) {
        CustomerID = c_id;
        EntryDate = 0; // TODO
        if (id < 2101) {
            CarrierID = random.UniformRandom(1, 10);
        } else {
            CarrierID = 0;
        }
        OrderLineCount = random.UniformRandom(5, 15);
        AllLocal = 1;
    }

    // For NewOrder transaction
    Order(RandomContext& random, uint32_t w_id) : WarehouseID(w_id) {
        DistrictID = random.UniformRandom(1, _districts_per_warehouse());
        CustomerID = random.NonUniformRandom(1023, 1, _customers_per_district());
        OrderLineCount = random.UniformRandom(5, 15);
        EntryDate = getDate();
        CarrierID = 0;
        // OrderID and AllLocal to be filled in by the transaction
    }

    Order(uint32_t w_id, uint16_t d_id, uint32_t o_id) : WarehouseID(w_id), DistrictID(d_id), OrderID(o_id) {}

    Order() = default;

    std::optional<uint32_t> WarehouseID;
    std::optional<uint32_t> DistrictID;
    std::optional<uint32_t> OrderID;
    std::optional<uint32_t> OrderLineCount;
    std::optional<uint64_t> EntryDate;
    std::optional<uint32_t> CustomerID;
    std::optional<uint32_t> CarrierID;
    std::optional<uint32_t> AllLocal; // boolean, 0 or 1

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, OrderID, OrderLineCount, EntryDate, CustomerID, 
        CarrierID, AllLocal);

private:
    k2::ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
    k2::ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class NewOrder {
public:
    static inline k2::dto::Schema neworder_schema {
		.name = "neworder",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT32T, "DID", false, false},
				{k2::dto::FieldType::UINT32T, "OID", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2 }
    };

    NewOrder(const Order& order) : WarehouseID(order.WarehouseID), 
            DistrictID(order.DistrictID), OrderID(order.OrderID) {}

    NewOrder() = default;

    std::optional<uint32_t> WarehouseID;
    std::optional<uint32_t> DistrictID;
    std::optional<uint32_t> OrderID;
    
    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, OrderID);
};  

class OrderLine {
public:
    static inline k2::dto::Schema orderline_schema {
		.name = "orderline",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT32T, "DID", false, false},
				{k2::dto::FieldType::UINT32T, "OID", false, false},
				{k2::dto::FieldType::UINT32T, "LineNumber", false, false},
				{k2::dto::FieldType::UINT64T, "DeliveryDate", false, false},
				{k2::dto::FieldType::UINT32T, "ItemID", false, false},
				{k2::dto::FieldType::UINT32T, "SupplyWID", false, false},
				{k2::dto::FieldType::FLOAT, "Amount", false, false},
				{k2::dto::FieldType::UINT32T, "Quantity", false, false},
				{k2::dto::FieldType::STRING, "DistInfo", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2, 3 }
    };

    // For initial population
    OrderLine(RandomContext& random, const Order& order, uint16_t line_num) :
            WarehouseID(order.WarehouseID), DistrictID(order.DistrictID), OrderID(order.OrderID), OrderLineNumber(line_num) {
        ItemID = random.UniformRandom(1, 100000);
        SupplyWarehouseID = WarehouseID;

        if (order.OrderID < 2101) {
            DeliveryDate = order.EntryDate;
            Amount = 0.0f;
        } else {
            DeliveryDate = 0;
            Amount = random.UniformRandom(1, 999999) / 100.0f;
        }

        Quantity = 5;
        DistInfo = random.RandomString(24, 24);
    }

    // For New-Order transaction
    // Amount and DistInfo must be filled in during transaction
    // ItemID must be changed if it needs to be a rollback transactiom
    OrderLine(RandomContext& random, const Order& order, uint16_t line_num, uint32_t max_warehouse_id) :
            WarehouseID(order.WarehouseID), DistrictID(order.DistrictID), OrderID(order.OrderID), OrderLineNumber(line_num) {
        ItemID = random.NonUniformRandom(8191, 1, 100000);

        uint32_t homeRoll = random.UniformRandom(1, 100);
        if (homeRoll == 1 && max_warehouse_id > 1) {
            do {
                SupplyWarehouseID = random.UniformRandom(1, max_warehouse_id);
            } while (SupplyWarehouseID == WarehouseID);
        } else {
            SupplyWarehouseID = WarehouseID;
        }

        Quantity = random.UniformRandom(1, 10);
        Amount = 0.0f;
    }

    OrderLine() = default;

    std::optional<uint32_t> WarehouseID;
    std::optional<uint32_t> DistrictID;
    std::optional<uint32_t> OrderID;
    std::optional<uint32_t> OrderLineNumber;
    std::optional<uint64_t> DeliveryDate;
    std::optional<uint32_t> ItemID;
    std::optional<uint32_t> SupplyWarehouseID;
    std::optional<float> Amount; // TODO
    std::optional<uint32_t> Quantity;
    std::optional<k2::String> DistInfo;

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, OrderID, OrderLineNumber, DeliveryDate, ItemID, 
        SupplyWarehouseID, Amount, Quantity, DistInfo);
};

class Item {
public:
    static const uint32_t InvalidID = 999999;
    static inline k2::dto::Schema item_schema {
		.name = "item",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT32T, "ImageID", false, false},
				{k2::dto::FieldType::FLOAT, "Price", false, false},
				{k2::dto::FieldType::STRING, "Name", false, false},
				{k2::dto::FieldType::STRING, "Info", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    Item(RandomContext& random, uint32_t id) : ItemID(id) {
        ImageID = random.UniformRandom(1, 10000);
        Name = random.RandomString(14, 24);
        Price = random.UniformRandom(100, 10000) / 100.0f;
        Info = random.RandomString(26, 50);
        uint32_t originalRoll = random.UniformRandom(1, 10);
        if (originalRoll == 1) {
            const char original[] = "ORIGINAL";
            uint32_t length = Info->size();
            uint32_t originalStart = random.UniformRandom(0, length-8);
            memcpy((char*)Info->c_str() + originalStart, original, 8);
        }
    }

    Item(uint32_t id) : ItemID(id) {}

    Item() = default;

    std::optional<uint32_t> ItemID;
    std::optional<uint32_t> ImageID;
    std::optional<float> Price; // TODO
    std::optional<k2::String> Name;
    std::optional<k2::String> Info;

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(ItemID, ImageID, Price, Name, Info);
};

class Stock {
public:
    static inline k2::dto::Schema stock_schema {
		.name = "stock",
		.version = 1,
		.fields = std::vector<k2::dto::SchemaField> {
				{k2::dto::FieldType::UINT32T, "ID", false, false},
				{k2::dto::FieldType::UINT32T, "ItemID", false, false},
				{k2::dto::FieldType::FLOAT, "YTD", false, false},
				{k2::dto::FieldType::UINT32T, "OrderCount", false, false},
				{k2::dto::FieldType::UINT32T, "RemoteCount", false, false},
				{k2::dto::FieldType::UINT32T, "Quantity", false, false},
				{k2::dto::FieldType::STRING, "Dist_01", false, false},
				{k2::dto::FieldType::STRING, "Dist_02", false, false},
				{k2::dto::FieldType::STRING, "Dist_03", false, false},
				{k2::dto::FieldType::STRING, "Dist_04", false, false},
				{k2::dto::FieldType::STRING, "Dist_05", false, false},
				{k2::dto::FieldType::STRING, "Dist_06", false, false},
				{k2::dto::FieldType::STRING, "Dist_07", false, false},
				{k2::dto::FieldType::STRING, "Dist_08", false, false},
				{k2::dto::FieldType::STRING, "Dist_09", false, false},
				{k2::dto::FieldType::STRING, "Dist_10", false, false},
				{k2::dto::FieldType::STRING, "Info", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    Stock(RandomContext& random, uint32_t w_id, uint32_t i_id) : WarehouseID(w_id), ItemID(i_id) {
        Quantity = random.UniformRandom(10, 100);
        Dist_01 = random.RandomString(24, 24);
        Dist_02 = random.RandomString(24, 24);
        Dist_03 = random.RandomString(24, 24);
        Dist_04 = random.RandomString(24, 24);
        Dist_05 = random.RandomString(24, 24);
        Dist_06 = random.RandomString(24, 24);
        Dist_07 = random.RandomString(24, 24);
        Dist_08 = random.RandomString(24, 24);
        Dist_09 = random.RandomString(24, 24);
        Dist_10 = random.RandomString(24, 24);
        YTD = 0.0f;
        OrderCount = 0;
        RemoteCount = 0;
        Info = random.RandomString(26, 50);
        uint32_t originalRoll = random.UniformRandom(1, 10);
        if (originalRoll == 1) {
            const char original[] = "ORIGINAL";
            uint32_t length = Info->size();
            uint32_t originalStart = random.UniformRandom(0, length-8);
            memcpy((char*)Info->c_str() + originalStart, original, 8);
        }
    }

    Stock(uint32_t w_id, uint32_t i_id) : WarehouseID(w_id), ItemID(i_id) {}

    Stock() = default;

    const char* getDistInfo(uint32_t d_id) {
        switch(d_id) {
            case 1:
                return Dist_01->c_str();
            case 2:
                return Dist_02->c_str();
            case 3:
                return Dist_03->c_str();
            case 4:
                return Dist_04->c_str();
            case 5:
                return Dist_05->c_str();
            case 6:
                return Dist_06->c_str();
            case 7:
                return Dist_07->c_str();
            case 8:
                return Dist_08->c_str();
            case 9:
                return Dist_09->c_str();
            case 10:
                return Dist_10->c_str();
            default:
                throw 0;
        }
    }

    std::optional<uint32_t> WarehouseID;
    std::optional<uint32_t> ItemID;
    std::optional<float> YTD; // TODO
    std::optional<uint32_t> OrderCount;
    std::optional<uint32_t> RemoteCount;
    std::optional<uint32_t> Quantity;
    std::optional<k2::String> Dist_01;
    std::optional<k2::String> Dist_02;
    std::optional<k2::String> Dist_03;
    std::optional<k2::String> Dist_04;
    std::optional<k2::String> Dist_05;
    std::optional<k2::String> Dist_06;
    std::optional<k2::String> Dist_07;
    std::optional<k2::String> Dist_08;
    std::optional<k2::String> Dist_09;
    std::optional<k2::String> Dist_10;
    std::optional<k2::String> Info;

    static inline thread_local seastar::lw_shared_ptr<k2::dto::Schema> schema;
    static inline k2::String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, ItemID, YTD, OrderCount, RemoteCount, Quantity, Dist_01, Dist_02,
        Dist_03, Dist_04, Dist_05, Dist_06, Dist_07, Dist_08, Dist_09, Dist_10, Info);
};

void setupSchemaPointers() {
    Warehouse::schema = seastar::make_lw_shared(Warehouse::warehouse_schema);
    District::schema = seastar::make_lw_shared(District::district_schema);
    Customer::schema = seastar::make_lw_shared(Customer::customer_schema);
    History::schema = seastar::make_lw_shared(History::history_schema);
    Order::schema = seastar::make_lw_shared(Order::order_schema);
    NewOrder::schema = seastar::make_lw_shared(NewOrder::neworder_schema);
    OrderLine::schema = seastar::make_lw_shared(OrderLine::orderline_schema);
    Item::schema = seastar::make_lw_shared(Item::item_schema);
    Stock::schema = seastar::make_lw_shared(Stock::stock_schema);
}

