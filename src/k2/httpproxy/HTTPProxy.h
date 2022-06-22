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
#include <skvhttp/common/Status.h>
#include <skvhttp/dto/Collection.h>
#include <skvhttp/dto/ControlPlaneOracle.h>
#include <skvhttp/dto/K23SI.h>


namespace k2 {
namespace sh=skv::http;
namespace shd=skv::http::dto;

// Priority queue based expiry timer to handle txns of different timeout
template <class T, class ClockT=Clock>
class ExpiryQueue {
public:
    void add(T val, TimePoint expiry) {
        _queue.push(std::make_pair(expiry, val));
    }

    // The FN takes element ID as parameter, returns empty if element is expired/removed.
    // If the element not expired because it's expiry time was updated, it returns the new expiry time
    typedef std::function<seastar::future<std::optional<TimePoint>>(T)> FN;

    void start(FN&& func) {
        _expiryTimer.setCallback([this, func=std::move(func)] {
            return seastar::do_until(
                [this] {return _queue.empty() || _queue.top().first > ClockT::now();},
                [this, func=std::move(func)]{
                // Copy top element value and remove
                T elem = _queue.top().second;
                _queue.pop();
                return func(elem)
                    .then([this, elem](auto new_expiry) {
                        if (new_expiry) {
                            _queue.push(std::make_pair(*new_expiry, elem));
                        }
                        return seastar::make_ready_future<>();
                    });
                });
        });
        _expiryTimer.armPeriodic(_minTimeout/2);
    }

    seastar::future<> stop() {
        return _expiryTimer.stop();
    }

private:
    typedef std::pair<TimePoint, T> ElemT;

    struct GreaterComp {
        constexpr bool operator()(const ElemT &lhs, const ElemT &rhs) const {return lhs.first > rhs.first;}
    };
    PeriodicTimer _expiryTimer;
    // Maximum supported resolution of expiry time.
    Duration _minTimeout{1s};
    // Min element on top, ordered by expiry time
    std::priority_queue<ElemT, std::vector<ElemT>, GreaterComp> _queue;
};

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
        Duration idleTimeout;
        TimePoint lastAccess;
    };

    // Called when txn is used by api. Updates last accessed without
    // rearranging the expiry queue. Queue will be rearranged by the timer function.
    void updateLastAccessed(ManagedTxn& txn) { txn.lastAccess = Clock::now();}

    std::unordered_map<shd::Timestamp, ManagedTxn> _txns;

    // shd schema cache:
    // collection name -> (schema name -> (schema version -> schemaPtr))
    std::unordered_map<sh::String,
        std::unordered_map<String,
            std::unordered_map<uint32_t, std::shared_ptr<shd::Schema>>
    >> _shdSchemas;
    ExpiryQueue<shd::Timestamp> _expiryQueue;

};  // class HTTPProxy

} // namespace k2
