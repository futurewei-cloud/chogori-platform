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

#include "PlogClient.h"
#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/Collection.h>
#include <k2/dto/Persistence.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RPCTypes.h>
#include <k2/transport/Status.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>

namespace k2 {

char Rand_alnum()
{
    char c;
    while (!std::isdigit(c = static_cast<char>(std::rand())));
    return c;
}

PlogClient::PlogClient() {
    K2INFO("dtor");
}

PlogClient::~PlogClient() {
    K2INFO("~dtor");
}

void PlogClient::GetPlogServerEndpoint(String plog_server_url) {
    K2INFO("Get new address" << plog_server_endpoints[0]);
    plog = RPC().getTXEndpoint(plog_server_url);
}

seastar::future<> 
PlogClient::GetPlogServerUrls(){
    _cpo = CPOClient(String(_cpo_url()));

    uint32_t PlogServerAmount = 1;
    return _cpo.GetPlogServer(Deadline<>(get_plog_server_deadline()), PlogServerAmount).
    then([this, PlogServerAmount] (auto&& result) {
        auto& [status, response] = result;

        if (!status.is2xxOK() || PlogServerAmount!=response.PlogServerAmount) {
            K2INFO("Failed to obtain Plog Servers" << status);
            return seastar::make_exception_future<>(std::runtime_error("Failed to obtain Plog Servers"));
        }

        plog_server_endpoints.clear();
        for (uint32_t i=0; i < response.PlogServerAmount; ++i){
            plog_server_endpoints.push_back(response.PlogServerEndpoints[i]);
            K2INFO("Client address: " << response.PlogServerEndpoints[i]);
        }
        GetPlogServerEndpoint(plog_server_endpoints[0]);
        return seastar::make_ready_future<>();
    });
}

seastar::future<std::tuple<Status, String>> PlogClient::create(){
    String plogId = generate_plogId();
    dto::PlogCreateRequest request{.plogId = plogId};
    Duration timeout = plog_request_timeout();

    return RPC().callRPC<dto::PlogCreateRequest, dto::PlogCreateResponse>(dto::Verbs::K23SI_PERSISTENT_CREATE, request, *plog, timeout).
    then([this, plogId] (auto&& result) {
        auto& [status, response] = result;

        return seastar::make_ready_future<std::tuple<Status, String> >(std::tuple<Status, String>(std::move(status), std::move(plogId)));
    });
}

seastar::future<std::tuple<Status, uint32_t>> PlogClient::append(String plogId, uint32_t offset, Payload payload){
    K2INFO("Payload Size" << payload.getSize());
    dto::PlogAppendRequest request{.plogId = std::move(plogId), .offset=offset, .payload=std::move(payload)};
    Duration timeout = plog_request_timeout();
    K2INFO("Payload Size and offset " << request.payload.getSize() << " " << request.offset);

    return RPC().callRPC<dto::PlogAppendRequest, dto::PlogAppendResponse>(dto::Verbs::K23SI_PERSISTENT_APPEND, request, *plog, timeout).
        then([this, &request] (auto&& result) {
            auto& [status, response] = result;
            
            return seastar::make_ready_future<std::tuple<Status, uint32_t> >(std::tuple<Status, uint32_t>(std::move(status), std::move(response.offset)));
        });
}

template <typename ClockT=Clock>
seastar::future<std::tuple<Status, dto::PlogReadResponse>> PlogClient::read(Deadline<ClockT> deadline, String&& plogId, uint32_t offset, uint32_t size){
    dto::PlogReadRequest request{.plogId = std::move(plogId), .offset=offset, .size=size};
    Duration timeout = std::min(deadline.getRemaining(), plog_request_timeout());

    return RPC().callRPC<dto::PlogReadRequest, dto::PlogReadResponse>(dto::Verbs::K23SI_PERSISTENT_READ, request, *plog, timeout).
        then([this, &request, deadline] (auto&& result) {
            auto& [status, response] = result;

            if (deadline.isOver()) {
                K2DEBUG("Deadline exceeded");
                status = Statuses::S408_Request_Timeout("create plog deadline exceeded");
                return RPCResponse(std::move(status), dto::PlogReadResponse());
            }

            return RPCResponse(std::move(status), std::move(response));
        });
}

template <typename ClockT=Clock>
seastar::future<std::tuple<Status, dto::PlogSealResponse>> PlogClient::seal(Deadline<ClockT> deadline, String&& plogId, uint32_t offset){
    dto::PlogSealRequest request{.plogId = std::move(plogId), .offset=offset};
    Duration timeout = std::min(deadline.getRemaining(), plog_request_timeout());

    return RPC().callRPC<dto::PlogSealRequest, dto::PlogSealResponse>(dto::Verbs::K23SI_PERSISTENT_SEAL, request, *plog, timeout).
        then([this, &request, deadline] (auto&& result) {
            auto& [status, response] = result;

            if (deadline.isOver()) {
                K2DEBUG("Deadline exceeded");
                status = Statuses::S408_Request_Timeout("create plog deadline exceeded");
                return RPCResponse(std::move(status), dto::PlogSealResponse());
            }

            return RPCResponse(std::move(status), std::move(response));
        });
}

String PlogClient::generate_plogId(){
    std::string appendix;
    appendix.resize(10);
    generate_n(std::back_inserter(appendix), 10, Rand_alnum);
    String plogId = "TPCC_CLIENT_plog_" + appendix;
    return plogId;
}
} // k2
