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
namespace sh=skv::http;

namespace k2 {

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
// TODO Once our types support multi-content-type conversions, use the object API
// for now, use the raw API
/*
    seastar::future<HTTPPayload> _handleBegin(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleEnd(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleRead(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleWrite(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleGetKeyString(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleCreateQuery(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleQuery(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleCreateSchema(HTTPPayload&& request);
    seastar::future<HTTPPayload> _handleGetSchema(HTTPPayload&& request);
*/
    seastar::future<std::tuple<sh::Status, sh::dto::CollectionCreateResponse>> _handleCreateCollection(
        sh::dto::CollectionCreateRequest&& request);
    void _registerAPI();
    void _registerMetrics();

    sm::metric_groups _metric_groups;
    uint64_t _deserializationErrors = 0;

    bool _stopped = true;
    k2::K23SIClient _client;
    uint64_t _txnID = 0;
    uint64_t _queryID = 0;
    std::unordered_map<uint64_t, k2::K2TxnHandle> _txns;
    // Store in progress queries
    std::unordered_map<uint64_t, Query> _queries;
    std::vector<seastar::future<>> _endFuts;
};  // class HTTPProxy

} // namespace k2
