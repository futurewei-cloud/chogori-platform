//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <string>

#include <k2/common/Common.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>

#include "tpcc_rand.h"
#include "mock/mock_k23si_client.h"

class SchemaType {
public:
    virtual k2::String getPartitionKey() = 0;
    virtual k2::String getRowKey() = 0;
    virtual void writeData(k2::Payload& payload) = 0;
};

struct Address {
    Address (RandomContext& random) {
        random.RandomString(10, 20, Street_1);
        random.RandomString(10, 20, Street_2);
        random.RandomString(10, 20, City);
        random.RandomString(2, 2, State);
        random.RandomZipString(Zip);
    }
    char Street_1[21];
    char Street_2[21];
    char City[21];
    char State[3];
    char Zip[10];
};

class Warehouse : public SchemaType {
public:
    Warehouse(RandomContext& random, uint32_t id) : WarehouseID(id) {
        random.RandomString(6, 10, data.Name);
        data.address = Address(random);
        data.Tax = random.UniformRandom(0, 2000) / 10000.0f;
        data.YTD = 300000.0f;
    }

    Warehouse(const k2::ReadResult& KV, uint32_t id) : WarehouseID(id) {
        KV.value.read(data);
    }

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return ""; }
    void writeData(k2::Payload& payload) { payload.write(data); }

    uint32_t WarehouseID;
    struct Data {
        float Tax; // TODO Needs to be fixed point to be in spec
        float YTD; // TODO Needs to be fixed point to be in spec
        char Name[11];
        Address address;
        K2_PAYLOAD_COPYABLE;
    } data;
};

class District : public SchemaType {
public:
    District(RandomContext& random, uint32_t w_id, uint16_t, id) : WarehouseID(w_id), DistrictID(id) {
        random.RandomString(6, 10, data.Name);
        data.address = Address(random);
        data.Tax = random.UniformRandom(0, 2000) / 10000.0f;
        data.YTD = 30000.0f;
        NextOID = 3001;
    }

    Warehouse(const k2::ReadResult& KV, uint32_t w_id, uint16_t id) : WarehouseID(w_id), DistrictID(id) {
        KV.value.read(data);
    }

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "DIST:" + std::to_string(DistrictID); }
    void writeData(k2::Payload& payload) { payload.write(data); }

    uint32_t WarehouseID;
    uint16_t DistrictID;
    struct Data {
        float Tax; // TODO Needs to be fixed point to be in spec
        float YTD; // TODO Needs to be fixed point to be in spec
        uint32_t NextOID;
        char Name[11];
        Address address;
        K2_PAYLOAD_COPYABLE;
    } data;
};

class Customer : public SchemaType {
public:
    Customer(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id) :
            WarehouseID(w_id), DistrictID(d_id), CustomerID(c_id) {
        random.RandomString(5, 5, data.LastName); // TODO needs to use special non-uniform function
        strcpy(data.MiddleName, "OE");
        random.RandomString(8, 16, data.FirstName);
        data.address = Address(random);
        random.RandomNumericString(16, 16, data.Phone);
        data.Date = std::duration_cast<std::microseconds>(std::steady_clock::now().time_since_epoch()).count();

        uint32_t creditRoll = random.UniformRandom(1, 10);
        if (creditRoll == 1) {
            strcpy(data.Credit, "BC");
        } else {
            strcpy(data.Credit, "GC");
        }

        data.CreditLimit = 50000.0f;
        data.Discount = random.UniformRandom(0, 5000) / 10000.0f;
        data.Balance = -10.0f;
        data.YTDPayment = 10.0f;
        data.PaymentCount = 1;
        data.DeliveryCount = 0;
        random.RandomString(300, 500, data.Info);
    }

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "CUST:" + std::to_string(DistrictID) + ":" + std::to_string(CustomerID); }
    void writeData(k2::Payload& payload) { payload.write(data); }

    uint32_t WarehouseID;
    uint16_t DistrictID;
    uint32_t CustomerID;
    struct Data {
        uint64_t SinceDate;
        float CreditLimit; // TODO Needs to be fixed point to be in spec
        float Discount;
        float Balance; // TODO Needs to be fixed point to be in spec
        float YTDPayment; // TODO Needs to be fixed point to be in spec
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
};

class History : public SchemaType {
public:
    History(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id) : WarehouseID(w_id) {
        data.CustomerID = c_id;
        data.CustomerWarehouseID = w_id;
        data.CustomerDistrictID = d_id;
        data.Date = std::duration_cast<std::microseconds>(std::steady_clock::now().time_since_epoch()).count();
        data.Amount = 10.0f;
        random.RandomString(12, 24, data.Info);
    }


    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "HIST:" + std::to_string(data.Date); }
    void writeData(k2::Payload& payload) { payload.write(data); }

    uint32_t WarehouseID;
    struct Data {
        uint64_t Date;
        uint32_t CustomerID;
        uint32_t CustomerWarehouseID;
        float Amount; // TODO
        uint16_t CustomerDistrictID;
        uint16_t DistrictID;
        char Info[25];
        K2_PAYLOAD_COPYABLE;
    } data;
};

class Order : public SchemaType {
    Order(RandomContext& random, uint32_t w_id, uint16_t d_id, uint32_t c_id, uint32_t id) :
            WarehouseID(w_id), DistrictID(d_id), OrderID(id) {
        data.CustomerID = c_id; // Note that this should be from a random permutation
        data.EntryDate = 0; // TODO
        if (id < 2101) {
            data.CarrierID = random.UniformRandom(1, 10);
        } else {
            data.CarrierID = 0;
        }
        data.OrderLineCount = random.UniformRandom(5, 15);
        data.AllLocal = true;
    }

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "ORDER:" + std::to_string(DistrictID) + ":" + std::to_string(OrderID); }
    void writeData(k2::Payload& payload) { payload.write(data); }

    uint32_t WarehouseID;
    uint32_t OrderID;
    uint16_t DistrictID;

    struct Data {
        uint64_t EntryDate;
        uint32_t CustomerID;
        uint16_t CarrierID;
        uint16_t OrderLineCount;
        bool AllLocal;
        K2_PAYLOAD_COPYABLE;
    } data;
};

class NewOrder : public SchemaType {
    NewOrder(const Order& order) : WarehouseID(order.WarehouseID), OrderID(order.OrderID), DistrictID(order.DistrictID) {}

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "NEW:" + std::to_string(DistrictID) + ":" + std::to_string(OrderID); }
    void writeData(k2::Payload& payload) { (void) payload; }

    uint32_t WarehouseID;
    uint32_t OrderID;
    uint16_t DistrictID;
};


class OrderLine : public SchemaType {
    // For initial population
    OrderLine(RandomContext& random, const Order& order, uint16_t line_num) :
            WarehouseID(order.WarehouseID), OrderID(order.OrderID), DistrictID(order.DistrictID), OrderLineNumber(line_num) {
        data.ItemID = random.UniformRandom(1, 100000);
        data.SupplyWarehouseID = WarehouseID;

        if (order.OrderID < 2101) {
            data.DeliveryDate = order.EntryDate;
            data.Amount = 0.0f;
        } else {
            data.DeliveryDate = 0;
            data.Amount = data.UniformRandom(1, 999999) / 100.0f;
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
        if (homeRoll == 1) {
            do {
                data.SupplyWarehouseID = random.UniformRandom(0, max_warehouse_id);
            } while (data.SupplyWarehouseID == WarehouseID);
        } else {
            data.SupplyWarehouseID = WarehouseID;
        }

        data.Quantity = random.UniformRandom(1, 10);
        data.Amount = 0.0f;
    }

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "ORDERLINE:" + std::to_string(DistrictID) + ":" + std::to_string(OrderID) + ":" + std::to_string(OrderLineNumber); }
    void writeData(k2::Payload& payload) { payload.write(data); }

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

class Item : public SchemaType {
public:
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

    k2::String getPartitionKey() { return "ITEM:" + std::to_string(ItemID); }
    k2::String getRowKey() { return ""; }
    void writeData(k2::Payload& payload) { payload.write(data); }

    uint32_t ItemID;

    struct Data {
        uint32_t ImageID;
        float Price; // TODO
        char Name[25];
        char Info[51];
        K2_PAYLOAD_COPYABLE;
    } data;
};

class Stock : public SchemaType {
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

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "STOCK:" + std::to_string(ItemID); }
    void writeData(k2::Payload& payload) { payload.write(data); }

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
};
