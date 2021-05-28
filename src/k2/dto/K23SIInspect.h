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

namespace k2 {
namespace dto {

// All of the inspect requests in this file are for test and debug purposes
// They return the current 3SI state without affecting it

// Requests all versions including WIs for a particular key
// without affecting transaction state
struct K23SIInspectRecordsRequest {
    // These fields make the request compatible with the PartitionRequest wrapper
    PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    Key key; // the key to gather all records for
    K2_PAYLOAD_FIELDS(pvid, collectionName, key);
    K2_DEF_FMT(K23SIInspectRecordsRequest, pvid, collectionName, key);
};

struct K23SIInspectRecordsResponse {
    std::vector<DataRecord> records;
    K2_PAYLOAD_FIELDS(records);
};

// Requests the TRH of a transaction without affecting transaction state
struct K23SIInspectTxnRequest {
    // These fields make the request compatible with the PartitionRequest wrapper
    PVID pvid; // the partition version ID. Should be coming from an up-to-date partition map
    String collectionName;
    Key key; // the key of the THR to request
    Timestamp timestamp;
    K2_PAYLOAD_FIELDS(pvid, collectionName, key, timestamp);
    K2_DEF_FMT(K23SIInspectTxnRequest, pvid, collectionName, key, timestamp);
};

// Contains the TRH data (struct TxnRecord) without the internal
// management members such as intrusive list hooks
struct K23SIInspectTxnResponse {
    // the MTR for the txn
    K23SI_MTR mtr;

    // the trh key for this txn
    Key trh;

    // the collection for the TR
    String trhCollection;

    // the ranges to which this transaction wrote. These are delivered as part of the End request and we have to ensure
    // that the corresponding write intents are converted appropriately
    std::unordered_map<String, std::unordered_set<KeyRangeVersion>> writeRanges;

    // Expiry time point for retention window - these are driven off each TSO clock update
    dto::Timestamp rwExpiry;

    bool syncFinalize = false;

    TxnRecordState state;

    dto::EndAction finalizeAction;

    K2_PAYLOAD_FIELDS(mtr, trh, trhCollection, writeRanges, rwExpiry, state, finalizeAction);
    K2_DEF_FMT(K23SIInspectTxnResponse, mtr, trh, trhCollection, writeRanges, rwExpiry, state, finalizeAction);
};

// Requests all WIs on a node for all keys
struct K23SIInspectWIsRequest {
    K2_PAYLOAD_EMPTY;
};

struct K23SIInspectWIsResponse {
    std::vector<WriteIntent> WIs;
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

// Request all keys stored on a node
struct K23SIInspectAllKeysRequest {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(K23SIInspectAllKeysRequest);
};

struct K23SIInspectAllKeysResponse {
    std::vector<Key> keys;
    K2_PAYLOAD_FIELDS(keys);
    K2_DEF_FMT(K23SIInspectAllKeysResponse, keys);
};

} // ns dto
} // ns k2
