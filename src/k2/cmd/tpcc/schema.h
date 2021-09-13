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

#include <decimal/decimal>
#include <string>

#include <k2/common/Common.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>

#include "tpcc_rand.h"
#include "Log.h"
using namespace k2;
static const String tpccCollectionName = "TPCC";

#define CHECK_READ_STATUS(read_result) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2LOG_D(log::tpcc, "TPC-C failed to read rows: {}", (read_result).status); \
            return make_exception_future(std::runtime_error(String("TPC-C failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

#define CHECK_READ_STATUS_TYPE(read_result,type) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2LOG_D(log::tpcc, "TPC-C failed to read rows: {}", (read_result).status); \
            return make_exception_future<type>(std::runtime_error(String("TPC-C failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

template<typename ValueType>
seastar::future<WriteResult> writeRow(ValueType& row, K2TxnHandle& txn, bool erase = false)
{
    return txn.write<ValueType>(row, erase).then([] (WriteResult&& result) {
        if (!result.status.is2xxOK()) {
            K2LOG_D(log::tpcc, "writeRow failed: {}", result.status);
            return seastar::make_exception_future<WriteResult>(std::runtime_error("writeRow failed!"));
        }

        return seastar::make_ready_future<WriteResult>(std::move(result));
    });
}

template<typename ValueType, typename FieldType>
seastar::future<PartialUpdateResult>
partialUpdateRow(ValueType& row, FieldType fieldsToUpdate, K2TxnHandle& txn) {
    return txn.partialUpdate<ValueType>(row, fieldsToUpdate).then([] (PartialUpdateResult&& result) {
        if (!result.status.is2xxOK()) {
            K2LOG_D(log::tpcc, "partialUpdateRow failed: {}", result.status);
            return seastar::make_exception_future<PartialUpdateResult>(std::runtime_error("partialUpdateRow failed!"));
        }

        return seastar::make_ready_future<PartialUpdateResult>(std::move(result));
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

    std::optional<String> Street_1;
    std::optional<String> Street_2;
    std::optional<String> City;
    std::optional<String> State;
    std::optional<String> Zip;

    SKV_RECORD_FIELDS(Street_1, Street_2, City, State, Zip);
};

uint64_t getDate()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

class Warehouse {
public:
    static inline dto::Schema warehouse_schema {
        .name = "warehouse",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::DECIMAL64, "Tax", false, false}, // Requires 4 digits of precision
                {dto::FieldType::DECIMAL64, "YTD", false, false}, // Requires 12 digits of precision
                {dto::FieldType::STRING, "Name", false, false},
                {dto::FieldType::STRING, "Street1", false, false},
                {dto::FieldType::STRING, "Street2", false, false},
                {dto::FieldType::STRING, "City", false, false},
                {dto::FieldType::STRING, "State", false, false},
                {dto::FieldType::STRING, "Zip", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };


    Warehouse(RandomContext& random, int16_t id) : WarehouseID(id) {
        Name = random.RandomString(6, 10);
        address = Address(random);
        Tax = random.UniformRandom(0, 2000);
        *Tax /= 10000;
        YTD = _districts_per_warehouse() * _customers_per_district() * 10;
    }

    Warehouse(int16_t id) : WarehouseID(id) {}

    Warehouse() = default;

    std::optional<int16_t> WarehouseID;
    std::optional<std::decimal::decimal64> Tax;
    std::optional<std::decimal::decimal64> YTD;
    std::optional<String> Name;
    Address address;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;

    SKV_RECORD_FIELDS(WarehouseID, Tax, YTD, Name, address);

private:
    ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
    ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class District {
public:
    static inline dto::Schema district_schema {
        .name = "district",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::INT16T, "DID", false, false},
                {dto::FieldType::DECIMAL64, "Tax", false, false}, // Requires 4 digits of precision
                {dto::FieldType::DECIMAL64, "YTD", false, false}, // Requires 12 digits of precision
                {dto::FieldType::INT64T, "NextOID", false, false},
                {dto::FieldType::STRING, "Name", false, false},
                {dto::FieldType::STRING, "Street1", false, false},
                {dto::FieldType::STRING, "Street2", false, false},
                {dto::FieldType::STRING, "City", false, false},
                {dto::FieldType::STRING, "State", false, false},
                {dto::FieldType::STRING, "Zip", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    District(RandomContext& random, int16_t w_id, int16_t id) : WarehouseID(w_id), DistrictID(id) {
        Name = random.RandomString(6, 10);
        address = Address(random);
        Tax = random.UniformRandom(0, 2000);
        *Tax /= 10000;
        YTD = _customers_per_district() * 10;
        NextOrderID = _customers_per_district()+1;
    }

    District(int16_t w_id, int16_t id) : WarehouseID(w_id), DistrictID(id) {}

    District() = default;

    std::optional<int16_t> WarehouseID;
    std::optional<int16_t> DistrictID;
    std::optional<std::decimal::decimal64> Tax;
    std::optional<std::decimal::decimal64> YTD;
    std::optional<int64_t> NextOrderID;
    std::optional<String> Name;
    Address address;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;

    SKV_RECORD_FIELDS(WarehouseID, DistrictID, Tax, YTD, NextOrderID, Name, address);

private:
   ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

class Customer {
public:
    static inline dto::Schema customer_schema {
        .name = "customer",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::INT16T, "DID", false, false},
                {dto::FieldType::INT32T, "CID", false, false},
                {dto::FieldType::INT64T, "SinceDate", false, false},
                {dto::FieldType::DECIMAL64, "CreditLimit", false, false}, //Requires 12 digits of precision
                {dto::FieldType::DECIMAL64, "Discount", false, false}, //Requires 4 digits of precision
                {dto::FieldType::DECIMAL64, "Balance", false, false}, //Requires 12 digits of precision
                {dto::FieldType::DECIMAL64, "YTDPayment", false, false}, //Requires 12 digits of precision
                {dto::FieldType::INT32T, "PaymentCount", false, false}, // Requires max >= 9999
                {dto::FieldType::INT32T, "DeliveryCount", false, false}, // Requires max >= 9999
                {dto::FieldType::STRING, "FirstName", false, false},
                {dto::FieldType::STRING, "MiddleName", false, false},
                {dto::FieldType::STRING, "LastName", false, false},
                {dto::FieldType::STRING, "Phone", false, false},
                {dto::FieldType::STRING, "Credit", false, false},
                {dto::FieldType::STRING, "Info", false, false},
                {dto::FieldType::STRING, "Street1", false, false},
                {dto::FieldType::STRING, "Street2", false, false},
                {dto::FieldType::STRING, "City", false, false},
                {dto::FieldType::STRING, "State", false, false},
                {dto::FieldType::STRING, "Zip", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2 }
    };

    Customer(RandomContext& random, int16_t w_id, int16_t d_id, int32_t c_id) :
            WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id) {
        if (c_id < 1000) {
            LastName = random.RandowLastNameString(true, c_id);
        } else {
            LastName = random.RandowLastNameString();
        }

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

        CreditLimit = 50000;
        Discount = random.UniformRandom(0, 5000);
        *Discount /= 10000;
        Balance = -10;
        YTDPayment = 10;
        PaymentCount = 1;
        DeliveryCount = 0;
        Info = random.RandomString(500, 500);
    }

    Customer(int16_t w_id, int16_t d_id, int32_t c_id) :
            WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id) {}

    Customer() = default;

    std::optional<int16_t> WarehouseID;
    std::optional<int16_t> DistrictID;
    std::optional<int32_t> CustomerID;
    std::optional<int64_t> SinceDate;
    std::optional<std::decimal::decimal64> CreditLimit;
    std::optional<std::decimal::decimal64> Discount;
    std::optional<std::decimal::decimal64> Balance;
    std::optional<std::decimal::decimal64> YTDPayment;
    std::optional<int32_t> PaymentCount;
    std::optional<int32_t> DeliveryCount;
    std::optional<String> FirstName;
    std::optional<String> MiddleName;
    std::optional<String> LastName;
    std::optional<String> Phone;
    std::optional<String> Credit; // "GC" or "BC"
    std::optional<String> Info;
    Address address;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, CustomerID, SinceDate, CreditLimit, Discount, Balance,
        YTDPayment, PaymentCount, DeliveryCount, FirstName, MiddleName, LastName, Phone, Credit,
        Info, address);
};

// secondary index for customer name in original TPCC created by the following ddl
//
//      CREATE INDEX idx_customer_name ON customer (c_w_id,c_d_id,c_last,c_first);
//
class IdxCustomerName {
public:
    static inline dto::Schema idx_customer_name_schema {
        .name = "idx_customer_name",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::INT16T, "ID", false, false},
            {dto::FieldType::INT16T, "DID", false, false},
            {dto::FieldType::STRING, "LastName", false, false},
            {dto::FieldType::INT32T, "CID", false, false}
        },
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2, 3 }
    };

    IdxCustomerName(int16_t w_id, int16_t d_id, String c_last, int32_t c_id) :
        WarehouseID(w_id), DistrictID(d_id), LastName(c_last), CustomerID(c_id) {};

    IdxCustomerName() = default;

    std::optional<int16_t> WarehouseID;
    std::optional<int16_t> DistrictID;
    std::optional<String> LastName;
    std::optional<int32_t> CustomerID;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, LastName, CustomerID);
};

class History {
public:
    static inline dto::Schema history_schema {
        .name = "history",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::INT64T, "Date", false, false},
                {dto::FieldType::INT32T, "CID", false, false},
                {dto::FieldType::INT16T, "CWID", false, false},
                {dto::FieldType::DECIMAL64, "Amount", false, false}, // Requires 6 digits of precision
                {dto::FieldType::INT16T, "CDID", false, false},
                {dto::FieldType::INT16T, "DID", false, false},
                {dto::FieldType::STRING, "Info", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    // For initial population
    History(RandomContext& random, int16_t w_id, int16_t d_id, int32_t c_id) : WarehouseID(w_id) {
        CustomerID = c_id;
        CustomerWarehouseID = w_id;
        CustomerDistrictID = d_id;
        Date = getDate();
        Amount = 10;
        Info = random.RandomString(12, 24);
    }

    // For payment transaction
    History(int16_t w_id, int16_t d_id, int32_t c_id, int16_t c_w_id, int16_t c_d_id,
                std::decimal::decimal64 amount, const char w_name[], const char d_name[]) : WarehouseID(w_id) {
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

    std::optional<int16_t> WarehouseID;
    std::optional<int64_t> Date;
    std::optional<int32_t> CustomerID;
    std::optional<int16_t> CustomerWarehouseID;
    std::optional<std::decimal::decimal64> Amount;
    std::optional<int16_t> CustomerDistrictID;
    std::optional<int16_t> DistrictID;
    std::optional<String> Info;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, Date, CustomerID, CustomerWarehouseID, Amount, CustomerDistrictID,
        DistrictID, Info);
};

class Order {
public:
    static inline dto::Schema order_schema {
        .name = "order",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::INT16T, "DID", false, false},
                {dto::FieldType::INT64T, "OID", false, false},
                {dto::FieldType::INT16T, "LineCount", false, false},
                {dto::FieldType::INT64T, "EntryDate", false, false},
                {dto::FieldType::INT32T, "CID", false, false},
                {dto::FieldType::INT32T, "CarrierID", false, false},
                {dto::FieldType::INT16T, "AllLocal", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2 }
    };

    // For initial population
    Order(RandomContext& random, int16_t w_id, int16_t d_id, int32_t c_id, int64_t id) :
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
    Order(RandomContext& random, int16_t w_id) : WarehouseID(w_id) {
        DistrictID = random.UniformRandom(1, _districts_per_warehouse());
        CustomerID = random.NonUniformRandom(1023, 1, _customers_per_district());
        OrderLineCount = random.UniformRandom(5, 15);
        EntryDate = getDate();
        CarrierID = 0;
        // OrderID and AllLocal to be filled in by the transaction
    }

    Order(int16_t w_id, int16_t d_id, int64_t o_id) : WarehouseID(w_id), DistrictID(d_id), OrderID(o_id) {}

    Order() = default;

    std::optional<int16_t> WarehouseID;
    std::optional<int16_t> DistrictID;
    std::optional<int64_t> OrderID;
    std::optional<int16_t> OrderLineCount;
    std::optional<int64_t> EntryDate;
    std::optional<int32_t> CustomerID;
    std::optional<int32_t> CarrierID;
    std::optional<int16_t> AllLocal; // boolean, 0 or 1

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, OrderID, OrderLineCount, EntryDate, CustomerID,
        CarrierID, AllLocal);

private:
    ConfigVar<uint16_t> _districts_per_warehouse{"districts_per_warehouse"};
    ConfigVar<uint32_t> _customers_per_district{"customers_per_district"};
};

// secondary index for order table on (warehouse_id, district_id, customer_id, and order_id)
class IdxOrderCustomer {
public:
    static inline dto::Schema idx_order_customer_schema {
        .name = "idx_order_customer",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::INT16T, "ID", false, false},
            {dto::FieldType::INT16T, "DID", false, false},
            {dto::FieldType::INT32T, "CID", false, false},
            {dto::FieldType::INT64T, "OID", false, false}
        },
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2, 3 }
    };

    IdxOrderCustomer(int16_t w_id, int16_t d_id, int32_t c_id, int64_t o_id) :
        WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id), OrderID(o_id) {};

    std::optional<int16_t> WarehouseID;
    std::optional<int16_t> DistrictID;
    std::optional<int32_t> CustomerID;
    std::optional<int64_t> OrderID;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, CustomerID, OrderID);
};

class NewOrder {
public:
    static inline dto::Schema neworder_schema {
        .name = "neworder",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::INT16T, "DID", false, false},
                {dto::FieldType::INT64T, "OID", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2 }
    };

    NewOrder(const Order& order) : WarehouseID(order.WarehouseID),
            DistrictID(order.DistrictID), OrderID(order.OrderID) {}

    // For Delivery Transaction
    NewOrder(int16_t w_id, int16_t d_id, int64_t o_id) : WarehouseID(w_id), DistrictID(d_id), OrderID(o_id) {}

    NewOrder() = default;

    std::optional<int16_t> WarehouseID;
    std::optional<int16_t> DistrictID;
    std::optional<int64_t> OrderID;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, OrderID);
};

class OrderLine {
public:
    static inline dto::Schema orderline_schema {
        .name = "orderline",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::INT16T, "DID", false, false},
                {dto::FieldType::INT64T, "OID", false, false},
                {dto::FieldType::INT32T, "LineNumber", false, false},
                {dto::FieldType::INT64T, "DeliveryDate", false, false},
                {dto::FieldType::INT32T, "ItemID", false, false},
                {dto::FieldType::INT16T, "SupplyWID", false, false},
                {dto::FieldType::DECIMAL64, "Amount", false, false}, // Requires 6 digits of precision
                {dto::FieldType::INT16T, "Quantity", false, false},
                {dto::FieldType::STRING, "DistInfo", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1, 2, 3 }
    };

    // For initial population
    OrderLine(RandomContext& random, const Order& order, int16_t line_num) :
            WarehouseID(order.WarehouseID), DistrictID(order.DistrictID), OrderID(order.OrderID), OrderLineNumber(line_num) {
        ItemID = random.UniformRandom(1, 100000);
        SupplyWarehouseID = WarehouseID;

        if (order.OrderID < 2101) {
            DeliveryDate = order.EntryDate;
            Amount = 0;
        } else {
            DeliveryDate = 0;
            Amount = random.UniformRandom(1, 999999);
            *Amount /= 100;
        }

        Quantity = 5;
        DistInfo = random.RandomString(24, 24);
    }

    // For New-Order transaction
    // Amount and DistInfo must be filled in during transaction
    // ItemID must be changed if it needs to be a rollback transactiom
    OrderLine(RandomContext& random, const Order& order, int16_t line_num, int16_t max_warehouse_id) :
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
        Amount = 0;
    }

    OrderLine(int16_t w_id, int16_t d_id, int64_t o_id, int32_t line_number) :
        WarehouseID(w_id), DistrictID(d_id), OrderID(o_id), OrderLineNumber(line_number) {}

    OrderLine() = default;

    std::optional<int16_t> WarehouseID;
    std::optional<int16_t> DistrictID;
    std::optional<int64_t> OrderID;
    std::optional<int32_t> OrderLineNumber;
    std::optional<int64_t> DeliveryDate;
    std::optional<int32_t> ItemID;
    std::optional<int16_t> SupplyWarehouseID;
    std::optional<std::decimal::decimal64> Amount;
    std::optional<int16_t> Quantity;
    std::optional<String> DistInfo;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, DistrictID, OrderID, OrderLineNumber, DeliveryDate, ItemID,
        SupplyWarehouseID, Amount, Quantity, DistInfo);
};

class Item {
public:
    static const int32_t InvalidID = 999999;
    static inline dto::Schema item_schema {
        .name = "item",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT32T, "ID", false, false},
                {dto::FieldType::INT32T, "ImageID", false, false},
                {dto::FieldType::DECIMAL64, "Price", false, false}, // Requires 5 digits of precision
                {dto::FieldType::STRING, "Name", false, false},
                {dto::FieldType::STRING, "Info", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> {}
    };

    Item(RandomContext& random, int32_t id) : ItemID(id) {
        ImageID = random.UniformRandom(1, 10000);
        Name = random.RandomString(14, 24);
        Price = random.UniformRandom(100, 10000);
        *Price /= 100;
        Info = random.RandomString(26, 50);
        uint32_t originalRoll = random.UniformRandom(1, 10);
        if (originalRoll == 1) {
            const char original[] = "ORIGINAL";
            uint32_t length = Info->size();
            uint32_t originalStart = random.UniformRandom(0, length-8);
            memcpy((char*)Info->c_str() + originalStart, original, 8);
        }
    }

    Item(int32_t id) : ItemID(id) {}

    Item() = default;

    std::optional<int32_t> ItemID;
    std::optional<int32_t> ImageID;
    std::optional<std::decimal::decimal64> Price;
    std::optional<String> Name;
    std::optional<String> Info;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(ItemID, ImageID, Price, Name, Info);
};

class Stock {
public:
    static inline dto::Schema stock_schema {
        .name = "stock",
        .version = 1,
        .fields = std::vector<dto::SchemaField> {
                {dto::FieldType::INT16T, "ID", false, false},
                {dto::FieldType::INT32T, "ItemID", false, false},
                {dto::FieldType::DECIMAL64, "YTD", false, false}, // Requires 8 digits of precision
                {dto::FieldType::INT16T, "OrderCount", false, false},
                {dto::FieldType::INT16T, "RemoteCount", false, false},
                {dto::FieldType::INT16T, "Quantity", false, false},
                {dto::FieldType::STRING, "Dist_01", false, false},
                {dto::FieldType::STRING, "Dist_02", false, false},
                {dto::FieldType::STRING, "Dist_03", false, false},
                {dto::FieldType::STRING, "Dist_04", false, false},
                {dto::FieldType::STRING, "Dist_05", false, false},
                {dto::FieldType::STRING, "Dist_06", false, false},
                {dto::FieldType::STRING, "Dist_07", false, false},
                {dto::FieldType::STRING, "Dist_08", false, false},
                {dto::FieldType::STRING, "Dist_09", false, false},
                {dto::FieldType::STRING, "Dist_10", false, false},
                {dto::FieldType::STRING, "Info", false, false}},
        .partitionKeyFields = std::vector<uint32_t> { 0 },
        .rangeKeyFields = std::vector<uint32_t> { 1 }
    };

    Stock(RandomContext& random, int16_t w_id, int32_t i_id) : WarehouseID(w_id), ItemID(i_id) {
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
        YTD = 0;
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

    Stock(int16_t w_id, int32_t i_id) : WarehouseID(w_id), ItemID(i_id) {}

    Stock() = default;

    const char* getDistInfo(int16_t d_id) {
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

    std::optional<int16_t> WarehouseID;
    std::optional<int32_t> ItemID;
    std::optional<std::decimal::decimal64> YTD;
    std::optional<int16_t> OrderCount;
    std::optional<int16_t> RemoteCount;
    std::optional<int16_t> Quantity;
    std::optional<String> Dist_01;
    std::optional<String> Dist_02;
    std::optional<String> Dist_03;
    std::optional<String> Dist_04;
    std::optional<String> Dist_05;
    std::optional<String> Dist_06;
    std::optional<String> Dist_07;
    std::optional<String> Dist_08;
    std::optional<String> Dist_09;
    std::optional<String> Dist_10;
    std::optional<String> Info;

    static inline thread_local std::shared_ptr<dto::Schema> schema;
    static inline String collectionName = tpccCollectionName;
    SKV_RECORD_FIELDS(WarehouseID, ItemID, YTD, OrderCount, RemoteCount, Quantity, Dist_01, Dist_02,
        Dist_03, Dist_04, Dist_05, Dist_06, Dist_07, Dist_08, Dist_09, Dist_10, Info);
};

void setupSchemaPointers() {
    Warehouse::schema = std::make_shared<dto::Schema>(Warehouse::warehouse_schema);
    District::schema = std::make_shared<dto::Schema>(District::district_schema);
    Customer::schema = std::make_shared<dto::Schema>(Customer::customer_schema);
    IdxCustomerName::schema = std::make_shared<dto::Schema>(IdxCustomerName::idx_customer_name_schema);
    IdxOrderCustomer::schema = std::make_shared<dto::Schema>(IdxOrderCustomer::idx_order_customer_schema);
    History::schema = std::make_shared<dto::Schema>(History::history_schema);
    Order::schema = std::make_shared<dto::Schema>(Order::order_schema);
    NewOrder::schema = std::make_shared<dto::Schema>(NewOrder::neworder_schema);
    OrderLine::schema = std::make_shared<dto::Schema>(OrderLine::orderline_schema);
    Item::schema = std::make_shared<dto::Schema>(Item::item_schema);
    Stock::schema = std::make_shared<dto::Schema>(Stock::stock_schema);
}
