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

#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/Status.h>

#include <nlohmann/json.hpp>

namespace k2 {
namespace dto {

// All of the inspect requests in this file are for test and debug purposes
// They return the current 3SI state without affecting it

// Requests all versions including WIs for a particular key
// without affecting transaction state
struct K23SIInspectRecordsRequest {
    // These fields make the request compatible with the PartitionRequest wrapper
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    Key key; // the key to gather all records for
    K2_PAYLOAD_FIELDS(pvid, collectionName, key);

    friend std::ostream& operator<<(std::ostream& os, const K23SIInspectRecordsRequest& r) {
        return os << "{pvid=" << r.pvid << ", colName=" << r.collectionName
                   << ", key=" << r.key << "}";
    }
};

struct K23SIInspectRecordsResponse {
    std::vector<DataRecord> records;
    K2_PAYLOAD_FIELDS(records);
};

// Requests the TRH of a transaction without affecting transaction state
struct K23SIInspectTxnRequest {
    // These fields make the request compatible with the PartitionRequest wrapper
    Partition::PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    Key key; // the key of the THR to request
    K23SI_MTR mtr;
    K2_PAYLOAD_FIELDS(pvid, collectionName, key, mtr);

    friend std::ostream& operator<<(std::ostream& os, const K23SIInspectTxnRequest& r) {
        return os << "{pvid=" << r.pvid << ", colName=" << r.collectionName
                  << ", mtr=" << r.mtr  << ", key=" << r.key << "}";
    }
};

void inline to_json(nlohmann::json& j, const K23SIInspectTxnRequest& req) {
    j = nlohmann::json{{"pvid", req.pvid},
                       {"collectionName", req.collectionName},
                       {"key", req.key},
                       {"mtr", req.mtr}};
}

void inline from_json(const nlohmann::json& j, K23SIInspectTxnRequest& req) {
    j.at("pvid").get_to(req.pvid);
    j.at("collectionName").get_to(req.collectionName);
    j.at("key").get_to(req.key);
    j.at("mtr").get_to(req.mtr);
}

// Contains the TRH data (struct TxnRecord) without the internal
// management members such as intrusive list hooks
struct K23SIInspectTxnResponse {
    TxnId txnId;

    // the keys to which this transaction wrote. These are delivered as part of the End request and we have to ensure
    // that the corresponding write intents are converted appropriately
    std::vector<dto::Key> writeKeys;

    // Expiry time point for retention window - these are driven off each TSO clock update
    dto::Timestamp rwExpiry;

    bool syncFinalize = false;

    TxnRecordState state;

    K2_PAYLOAD_FIELDS(txnId, writeKeys, rwExpiry, state);

    friend std::ostream& operator<<(std::ostream& os, const K23SIInspectTxnResponse& rec) {
        os << "{txnId=" << rec.txnId << ", writeKeys=[";
        os << rec.writeKeys.size();
        os << "], rwExpiry=" << rec.rwExpiry << ", syncfin=" << rec.syncFinalize << "}";
        return os;
    }
};

void inline to_json(nlohmann::json& j, const K23SIInspectTxnResponse& resp) {
    j = nlohmann::json{{"txnId", resp.txnId},
                       {"writeKeys", resp.writeKeys},
                       {"rwExpiry", resp.rwExpiry},
                       {"syncFinalize", resp.syncFinalize},
                       {"state", resp.state}};
}

void inline from_json(const nlohmann::json& j, K23SIInspectTxnResponse& resp) {
    j.at("txnId").get_to(resp.txnId);
    j.at("writeKeys").get_to(resp.writeKeys);
    j.at("rwExpiry").get_to(resp.rwExpiry);
    j.at("syncFinalize").get_to(resp.syncFinalize);
    j.at("state").get_to(resp.state);
}

// Requests all WIs on a node for all keys
struct K23SIInspectWIsRequest {
    K2_PAYLOAD_EMPTY;
};

struct K23SIInspectWIsResponse {
    std::vector<DataRecord> WIs;
    K2_PAYLOAD_FIELDS(WIs);
};

// Request all TRHs on a node
struct K23SIInspectAllTxnsRequest {
    K2_PAYLOAD_EMPTY;
};

struct K23SIInspectAllTxnsResponse {
    std::vector<K23SIInspectTxnResponse> txns;
    K2_PAYLOAD_FIELDS(txns);
};

// This request object is empty, just need the no-op function overload for compilation
void inline to_json(nlohmann::json&, const K23SIInspectAllTxnsRequest&) {}

// This request object is empty, just need the no-op function overload for compilation
void inline from_json(const nlohmann::json&, K23SIInspectAllTxnsRequest&) {}

void inline to_json(nlohmann::json& j, const K23SIInspectAllTxnsResponse& resp) {
    j = nlohmann::json{{"txns", resp.txns}};
}

void inline from_json(const nlohmann::json& j, K23SIInspectAllTxnsResponse& resp) {
    j.at("txns").get_to(resp.txns);
}

// Request all keys stored on a node
struct K23SIInspectAllKeysRequest {
    K2_PAYLOAD_EMPTY;
};

struct K23SIInspectAllKeysResponse {
    std::vector<Key> keys;
    K2_PAYLOAD_FIELDS(keys);
};

// This request object is empty, just need the no-op function overload for compilation
void inline to_json(nlohmann::json&, const K23SIInspectAllKeysRequest&) {}

// This request object is empty, just need the no-op function overload for compilation
void inline from_json(const nlohmann::json&, K23SIInspectAllKeysRequest&) {}

void inline to_json(nlohmann::json& j, const K23SIInspectAllKeysResponse& resp) {
    j = nlohmann::json{{"keys", resp.keys}};
}

void inline from_json(const nlohmann::json& j, K23SIInspectAllKeysResponse& resp) {
    j.at("keys").get_to(resp.keys);
}

} // ns dto
} // ns k2

