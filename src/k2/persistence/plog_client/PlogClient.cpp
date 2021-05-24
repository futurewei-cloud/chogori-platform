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
#include <random>

namespace k2 {

PlogClient::PlogClient() {
    K2LOG_I(log::plogcl, "dtor");
}

PlogClient::~PlogClient() {
    K2LOG_I(log::plogcl, "~dtor");
}


seastar::future<>
PlogClient::init(String clusterName, String cpoUrl){
    _cpo = CPOClient(cpoUrl);
    return _getPersistenceCluster(clusterName);
}

seastar::future<>
PlogClient::_getPlogServerEndpoints() {
    for(auto& v : _persistenceCluster.persistenceGroupVector){
        K2LOG_D(log::plogcl, "Persistence Group: {}", v.name);
        _persistenceNameMap[v.name] = _persistenceNameList.size();
        _persistenceNameList.push_back(v.name);

        std::vector<std::unique_ptr<TXEndpoint>> endpoints;
        for (auto& url: v.plogServerEndpoints){
            K2LOG_D(log::plogcl, "Plog Server Url: {}", url);
            auto ep = RPC().getTXEndpoint(url);
            if (ep){
                endpoints.push_back(std::move(ep));
            }
            else{
                K2LOG_D(log::plogcl, "Cannot obtain endpoint from URL: {}", url);
            }
        }
        if (endpoints.size() == 0){
            K2LOG_D(log::plogcl, "Failed to obtain the Endpoint of Plog Servers");
            return seastar::make_exception_future<>(std::runtime_error("Failed to obtain the Endpoint of Plog Servers"));
        }
        _persistenceMapEndpoints[std::move(v.name)] = std::move(endpoints);
    }
    return seastar::make_ready_future<>();
}

seastar::future<>
PlogClient::_getPersistenceCluster(String clusterName){
    return _cpo.GetPersistenceCluster(Deadline<>(_cpo_timeout()), std::move(clusterName)).
    then([this] (auto&& result) {
        auto& [status, response] = result;

        if (!status.is2xxOK()) {
            K2LOG_E(log::plogcl, "Failed to obtain Persistence Cluster {}", status);
            return seastar::make_exception_future<>(std::runtime_error("Failed to obtain Persistence Cluster"));
        }

        _persistenceCluster = std::move(response.cluster);
        _persistenceMapPointer = rand() % _persistenceCluster.persistenceGroupVector.size();
        _persistenceMapEndpoints.clear();
        return _getPlogServerEndpoints();
    });
}

// TODO: If the create call fails, we should try and create the plog in another persistence group.
seastar::future<std::tuple<Status, String>> PlogClient::create(uint8_t retries){
    String plogId = _generatePlogId();
    dto::PlogCreateRequest request{.plogId = plogId};
    std::vector<seastar::future<std::tuple<Status, dto::PlogCreateResponse> > > createFutures;
    for (auto& ep:_persistenceMapEndpoints[_persistenceNameList[_persistenceMapPointer]]){
        createFutures.push_back(RPC().callRPC<dto::PlogCreateRequest, dto::PlogCreateResponse>(dto::Verbs::PLOG_CREATE, request, *ep, _plog_timeout()));
    }
    return seastar::when_all_succeed(createFutures.begin(), createFutures.end())
        .then([this, plogId, retries](std::vector<std::tuple<Status, dto::PlogCreateResponse> >&& results) { 
            Status return_status;
            for (auto& result: results){
                auto& [status, response] = result;
                return_status = std::move(status);
                if (!return_status.is2xxOK()) 
                    break;
            }
            if (return_status.code == 409 && retries > 0){
                    return create(retries-1);
            }
            return seastar::make_ready_future<std::tuple<Status, String> >(std::tuple<Status, String>(std::move(return_status), std::move(plogId)));
        });
}

seastar::future<std::tuple<Status, uint32_t>> PlogClient::append(String plogId, uint32_t offset, Payload payload){
    uint32_t expected_offset = offset + payload.getSize();
    uint32_t appended_offset = payload.getSize();
    dto::PlogAppendRequest request{.plogId = std::move(plogId), .offset=offset, .payload=std::move(payload)};

    std::vector<seastar::future<std::tuple<Status, dto::PlogAppendResponse> > > appendFutures;
    for (auto& ep:_persistenceMapEndpoints[_persistenceNameList[_persistenceMapPointer]]){
        appendFutures.push_back(RPC().callRPC<dto::PlogAppendRequest, dto::PlogAppendResponse>(dto::Verbs::PLOG_APPEND, request, *ep, _plog_timeout()));
    }

    return seastar::when_all_succeed(appendFutures.begin(), appendFutures.end())
        .then([this, expected_offset, appended_offset](std::vector<std::tuple<Status, dto::PlogAppendResponse> >&& results) { 
            Status return_status;
            for (auto& result: results){
                auto& [status, response] = result;
                return_status = std::move(status);
                if (!return_status.is2xxOK()) 
                    break;
                if (response.newOffset != expected_offset){
                    return_status = Statuses::S500_Internal_Server_Error("offset inconsistent");
                    break;
                }
            }
            return seastar::make_ready_future<std::tuple<Status, uint32_t> >(std::tuple<Status, uint32_t>(std::move(return_status), std::move(expected_offset)));
        });
}


seastar::future<std::tuple<Status, Payload>> PlogClient::read(String plogId, uint32_t offset, uint32_t size){
    dto::PlogReadRequest request{.plogId = std::move(plogId), .offset=offset, .size=size};

    return RPC().callRPC<dto::PlogReadRequest, dto::PlogReadResponse>(dto::Verbs::PLOG_READ, request, *_persistenceMapEndpoints[_persistenceNameList[_persistenceMapPointer]][0], _plog_timeout()).
        then([this] (auto&& result) {
            auto& [status, response] = result;

            return seastar::make_ready_future<std::tuple<Status, Payload> >(std::tuple<Status, Payload>(std::move(status), std::move(response.payload)));
        });
}

seastar::future<std::tuple<Status, uint32_t>> PlogClient::seal(String plogId, uint32_t offset){
    dto::PlogSealRequest request{.plogId = std::move(plogId), .truncateOffset=offset};

    std::vector<seastar::future<std::tuple<Status, dto::PlogSealResponse> > > sealFutures;
    for (auto& ep:_persistenceMapEndpoints[_persistenceNameList[_persistenceMapPointer]]){
        sealFutures.push_back(RPC().callRPC<dto::PlogSealRequest, dto::PlogSealResponse>(dto::Verbs::PLOG_SEAL, request, *ep, _plog_timeout()));
    }

    return seastar::when_all_succeed(sealFutures.begin(), sealFutures.end())
        .then([this](std::vector<std::tuple<Status, dto::PlogSealResponse> >&& results) { 
            Status return_status;
            uint32_t sealed_offset;
            for (auto& result: results){
                auto& [status, response] = result;
                return_status = std::move(status);
                sealed_offset = response.sealedOffset;
                if (!return_status.is2xxOK()) 
                    break;
            }
            return seastar::make_ready_future<std::tuple<Status, uint32_t> >(std::tuple<Status, uint32_t>(std::move(return_status), std::move(sealed_offset)));
        });
}

seastar::future<std::tuple<Status, std::tuple<uint32_t, bool>>> PlogClient::getPlogStatus(String plogId){
    dto::PlogGetStatusRequest request{.plogId = std::move(plogId)};

    std::vector<seastar::future<std::tuple<Status, dto::PlogGetStatusResponse> > > statusFutures;
    for (auto& ep:_persistenceMapEndpoints[_persistenceNameList[_persistenceMapPointer]]){
        statusFutures.push_back(RPC().callRPC<dto::PlogGetStatusRequest, dto::PlogGetStatusResponse>(dto::Verbs::PLOG_GET_STATUS, request, *ep, _plog_timeout()));
    }

    return seastar::when_all_succeed(statusFutures.begin(), statusFutures.end())
        .then([this](std::vector<std::tuple<Status, dto::PlogGetStatusResponse> >&& results) { 
            Status return_status;
            uint32_t current_offset=UINT_MAX;
            bool sealed=false;
            for (auto& result: results){
                auto& [status, response] = result;
                return_status = std::move(status);
                if (current_offset > response.currentOffset){
                    if (current_offset != UINT_MAX){
                        K2LOG_W(log::plogcl, "Plog Offset Inconsistent");
                    }
                    current_offset = response.currentOffset;
                }
                if (response.sealed){
                    sealed = true;
                }
                if (!return_status.is2xxOK())
                    break;
            }
            return seastar::make_ready_future<std::tuple<Status, std::tuple<uint32_t, bool>> >(std::tuple<Status, std::tuple<uint32_t, bool>>(std::move(return_status), std::make_tuple(std::move(current_offset), std::move(sealed))));
        });
}


// TODO: change the method to generate the random plog id later
String PlogClient::_generatePlogId(){
    String plogid = "TPCC_CLIENT_plog_0123456789";
    std::mt19937 g(std::rand());
    std::shuffle(plogid.begin()+18, plogid.end(), g);
    return plogid;
}

bool PlogClient::selectPersistenceGroup(String name){
    auto iter = _persistenceNameMap.find(name);
    if (iter == _persistenceNameMap.end()) {
        return false;
    }
    _persistenceMapPointer = iter->second;
    return true;
}

} // k2
