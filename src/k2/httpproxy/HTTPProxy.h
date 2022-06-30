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
#include <k2/infrastructure/APIServer.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/common/ExpiryList.h>
#include <skvhttp/common/Status.h>
#include <skvhttp/dto/Collection.h>
#include <skvhttp/dto/ControlPlaneOracle.h>
#include <skvhttp/dto/K23SI.h>

namespace k2 {
namespace sh=skv::http;
namespace shd=skv::http::dto;

// Utility function to generate e response of a given type
template <typename ...T>
inline seastar::future<sh::Response<T...>> MakeHTTPResponse(sh::Status s, T&&... r) {
    return seastar::make_ready_future<sh::Response<T...>>(sh::Response<T...>(std::move(s), std::forward<T>(r)...));
}
// Utility function to generate e response of a given type
template <typename... T>
inline seastar::future<sh::Response<T...>> MakeHTTPResponse(sh::Response<T...>&& resp) {
    return seastar::make_ready_future<sh::Response<T...>>(std::forward(resp));
}

class HTTPProxy {
public:  // application lifespan
    HTTPProxy();
    seastar::future<> gracefulStop();
    seastar::future<> start();

private:
/*
    TODO: remaining ops
    seastar::future<HTTPPayload> _handleGetSchema(HTTPPayload&& request);
*/
    seastar::future<std::tuple<sh::Status, sh::dto::CollectionCreateResponse>>
        _handleCreateCollection(sh::dto::CollectionCreateRequest&& request);

    seastar::future<std::tuple<sh::Status, sh::dto::CreateSchemaResponse>>
        _handleCreateSchema(sh::dto::CreateSchemaRequest&& request);

    seastar::future<std::tuple<sh::Status, sh::dto::GetSchemaResponse>>
        _handleGetSchema(sh::dto::GetSchemaRequest&& request);

    seastar::future<std::tuple<sh::Status, sh::dto::TxnBeginResponse>>
        _handleTxnBegin(sh::dto::TxnBeginRequest&& request);

    seastar::future<std::tuple<sh::Status, sh::dto::WriteResponse>>
        _handleWrite(sh::dto::WriteRequest&& request);

    seastar::future<std::tuple<sh::Status, shd::WriteResponse>>
        _handleWrite(K2TxnHandle& txn, shd::WriteRequest&& request, dto::SKVRecord&& k2record);

    seastar::future<std::tuple<sh::Status, shd::WriteResponse>>
        _handlePartialUpdate(K2TxnHandle& txn, shd::WriteRequest&& request, dto::SKVRecord&& k2record);


    seastar::future<std::tuple<sh::Status, sh::dto::ReadResponse>>
        _handleRead(sh::dto::ReadRequest&& request);

    seastar::future<std::tuple<sh::Status, sh::dto::QueryResponse>>
        _handleQuery(sh::dto::QueryRequest&& request);

    seastar::future<std::tuple<sh::Status, sh::dto::TxnEndResponse>>
        _handleTxnEnd(sh::dto::TxnEndRequest&& request);

    void shdStorageToK2Record(const sh::String& collectionName, shd::SKVRecord::Storage&& key, dto::SKVRecord& k2record);
    seastar::future<std::tuple<sh::Status, sh::dto::CreateQueryResponse>>
        _handleCreateQuery(sh::dto::CreateQueryRequest&& request);


    seastar::future<std::tuple<k2::Status, std::shared_ptr<k2::dto::Schema>, std::shared_ptr<shd::Schema>>>
        _getSchemas(sh::String cname, sh::String sname, int64_t sversion);
    std::shared_ptr<shd::Schema> getSchemaFromCache(const sh::String& cname, std::shared_ptr<dto::Schema> schema);

    void _registerAPI();
    void _registerMetrics();

    sm::metric_groups _metric_groups;

    k2::K23SIClient _client;
    uint64_t _queryID = 0;

    struct ManagedTxn {
        k2::K2TxnHandle handle;
        std::unordered_map<uint64_t, Query> queries;
        TimePoint expiryTime;
        TimePoint expiry() {return expiryTime;}  // Used by ExpiryList
        nsbi::list_member_hook<> tsLink;  // Used by ExpiryList
        shd::Timestamp timestamp; // Used to remove element from _txns
    };

    void updateExpiry(ManagedTxn& txn) {
        txn.expiryTime = Clock::now() + _txnTimeout();
        _expiryList.moveToEnd(txn);
    }

    std::unordered_map<shd::Timestamp, ManagedTxn> _txns;

    // shd schema cache:
    // collection name -> (schema name -> (schema version -> schemaPtr))
    std::unordered_map<sh::String,
        std::unordered_map<String,
            std::unordered_map<uint32_t, std::shared_ptr<shd::Schema>>
    >> _shdSchemas;
    ExpiryList<ManagedTxn, &ManagedTxn::tsLink> _expiryList;
    // Txn idle timeout
    ConfigDuration _txnTimeout{"httpproxy_txn_timeout", 60s};
    // How often periodic timer runs to check for expiry. After expiry cleanup may take additional this time.

    ConfigDuration _expiryTimerInterval{"httpproxy_expiry_timer_interval", 10s};
};  // class HTTPProxy

} // namespace k2
