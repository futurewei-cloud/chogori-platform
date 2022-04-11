/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

#include <k2/module/k23si/client/k23si_client.h>

namespace k2 {
namespace dto {
// Request to get a schema
struct GetSchemaRequest {
    String collectionName;
    String schemaName;
    uint32_t schemaVersion = 0;

    K2_PAYLOAD_FIELDS(collectionName, schemaName, schemaVersion);
    K2_DEF_FMT(GetSchemaRequest, collectionName, schemaName, schemaVersion);
};
}


class HTTPProxy {
public:  // application lifespan
    HTTPProxy();
    seastar::future<> gracefulStop();
    seastar::future<> start();

private:
    static void serializeRecordFromJSON(k2::dto::SKVRecord& record, nlohmann::json&& jsonRecord);
    static nlohmann::json serializeJSONFromRecord(k2::dto::SKVRecord& record);

    seastar::future<nlohmann::json> _handleBegin(nlohmann::json&& request);
    seastar::future<nlohmann::json> _handleEnd(nlohmann::json&& request);
    seastar::future<nlohmann::json> _handleRead(nlohmann::json&& request);
    seastar::future<nlohmann::json> _handleWrite(nlohmann::json&& request);
    seastar::future<std::tuple<k2::Status, dto::CreateSchemaResponse>> _handleCreateSchema(
        dto::CreateSchemaRequest&& request);
    seastar::future<std::tuple<k2::Status, dto::Schema>> _handleGetSchema(dto::GetSchemaRequest&& request);

    void _registerAPI();
    void _registerMetrics();

    sm::metric_groups _metric_groups;
    k2::ExponentialHistogram _readLatency;
    k2::ExponentialHistogram _writeLatency;
    k2::ExponentialHistogram _txnLatency;
    k2::ExponentialHistogram _endLatency;

    uint64_t _totalTxns=0;
    uint64_t _abortedTxns=0;
    uint64_t _committedTxns=0;
    uint64_t _totalReads = 0;
    uint64_t _successReads = 0;
    uint64_t _failReads = 0;
    uint64_t _totalWrites = 0;
    uint64_t _successWrites = 0;
    uint64_t _failWrites = 0;

    bool _stopped = true;
    k2::K23SIClient _client;
    uint64_t _txnID = 0;
    std::unordered_map<uint64_t, k2::K2TxnHandle> _txns;
    std::vector<seastar::future<>> _endFuts;
    ConfigVar<uint32_t> _numPartitions{"num_partitions"};
};  // class HTTPProxy

} // namespace k2
