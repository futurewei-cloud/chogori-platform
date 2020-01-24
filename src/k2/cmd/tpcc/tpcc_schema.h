//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <string>

#include <k2/common/Common.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>

#include "tpcc_rand.h"

class SchemaType {
public:
    virtual k2::String getPartitionKey() = 0;
    virtual k2::String getRowKey() = 0;
    virtual void writeData(k2::Payload& payload) = 0;
};

class Warehouse : public SchemaType {
public:
    Warehouse(RandomContext& random, uint32_t id) : WarehouseID(id) {
        random.RandomString(6, 10, data.Name);
        random.RandomString(10, 20, data.Street_1);
        random.RandomString(10, 20, data.Street_2);
        random.RandomString(10, 20, data.City);
        random.RandomString(2, 2, data.State);
        random.RandomZipString(data.Zip);
        data.Tax = random.UniformRandom(0, 2000) / 10000.0f;
        data.YTD = 300000.0f;
    }

    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return ""; }
    void writeData(k2::Payload& payload) { payload.write(data); }

    uint32_t WarehouseID;
    struct Data {
        float Tax; // TODO Needs to be fixed point to be in spec
        float YTD; // TODO Needs to be fixed point to be in spec
        char Name[11];
        char Street_1[21];
        char Street_2[21];
        char City[21];
        char State[3];
        char Zip[10];
        K2_PAYLOAD_COPYABLE;
    } data;
};

class District : public SchemaType {
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
        char Street_1[21];
        char Street_2[21];
        char City[21];
        char State[3];
        char Zip[10];
        K2_PAYLOAD_COPYABLE;
    } data;
};

class Customer : public SchemaType {
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
        char Street_1[21];
        char Street_2[21];
        char City[21];
        char State[3];
        char Zip[10];
        char Phone[17];
        char Credit[3]; // "GC" or "BC"
        char Info[500];
        K2_PAYLOAD_COPYABLE;
    } data;
};

class History : public SchemaType {
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

class NewOrder : public SchemaType {
    k2::String getPartitionKey() { return std::to_string(WarehouseID); }
    k2::String getRowKey() { return "NEW:" + std::to_string(DistrictID) + ":" + std::to_string(OrderID); }
    void writeData(k2::Payload& payload) { (void) payload; }

    uint32_t WarehouseID;
    uint32_t OrderID;
    uint16_t DistrictID;
};

class Order : public SchemaType {
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

class OrderLine : public SchemaType {
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
    k2::String getPartitionKey() { return std::to_string(ItemID); }
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
