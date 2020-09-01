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
#include <k2/dto/Plog.h>
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
    K2INFO("dtor");
}

PlogClient::~PlogClient() {
    K2INFO("~dtor");
}

seastar::future<> 
PlogClient::getPlogPartitionMap() {
    for(auto& v : _partitionCluster.partitionGroupVector){
        K2INFO("Partition Group: " << v.name);
        _partitionNameMap[v.name] = _partitionNameList.size();
        _partitionNameList.push_back(v.name);
        
        std::vector<std::unique_ptr<TXEndpoint>> endpoints;
        for (auto& url: v.plogServerEndpoints){
            K2INFO("Plog Server Url: " << url);
            auto ep = RPC().getTXEndpoint(url);
            if (ep){
                endpoints.push_back(std::move(ep));
            }
        }
        if (endpoints.size() == 0){
            K2INFO("Failed to obtain the Endpoint of Plog Servers");
            return seastar::make_exception_future<>(std::runtime_error("Failed to obtain the Endpoint of Plog Servers"));
        }
        _partitionMapEndpoints[std::move(v.name)] = std::move(endpoints);
    }
    return seastar::make_ready_future<>();
}

seastar::future<> 
PlogClient::getPartitionCluster(String name){
    _cpo = CPOClient(String(_cpo_url()));
    return _cpo.GetPartitionCluster(Deadline<>(_cpo_timeout()), std::move(name)).
    then([this] (auto&& result) {
        auto& [status, response] = result;

        if (!status.is2xxOK()) {
            K2INFO("Failed to obtain Partition Cluster" << status);
            return seastar::make_exception_future<>(std::runtime_error("Failed to obtain Partition Cluster Map"));
        }

        _partitionCluster = std::move(response.cluster);
        _partition_map_pointer = rand() % _partitionCluster.partitionGroupVector.size();
        _partitionMapEndpoints.clear();
        return getPlogPartitionMap();
    });
}

seastar::future<std::tuple<Status, String>> PlogClient::create(){
    String plogId = generate_plogId();
    dto::PlogCreateRequest request{.plogId = plogId};
    
    std::vector<seastar::future<std::tuple<Status, dto::PlogCreateResponse> > > createFutures;
    for (auto& ep:_partitionMapEndpoints[_partitionNameList[_partition_map_pointer]]){
        createFutures.push_back(RPC().callRPC<dto::PlogCreateRequest, dto::PlogCreateResponse>(dto::Verbs::K23SI_PERSISTENT_CREATE, request, *ep, _plog_timeout()));
    }
    return seastar::when_all_succeed(createFutures.begin(), createFutures.end())
        .then([this, plogId](std::vector<std::tuple<Status, dto::PlogCreateResponse> >&& results) { 
            Status return_status;
            for (auto& result: results){
                auto& [status, response] = result;
                return_status = std::move(status);
                if (!return_status.is2xxOK()) 
                    break;
            }
            return seastar::make_ready_future<std::tuple<Status, String> >(std::tuple<Status, String>(std::move(return_status), std::move(plogId)));
        });
}

seastar::future<std::tuple<Status, uint32_t>> PlogClient::append(String plogId, uint32_t offset, Payload payload){
    uint32_t expected_offset = offset + payload.getSize();
    uint32_t appended_offset = payload.getSize();
    dto::PlogAppendRequest request{.plogId = std::move(plogId), .offset=offset, .payload=std::move(payload)};

    std::vector<seastar::future<std::tuple<Status, dto::PlogAppendResponse> > > appendFutures;
    for (auto& ep:_partitionMapEndpoints[_partitionNameList[_partition_map_pointer]]){
        appendFutures.push_back(RPC().callRPC<dto::PlogAppendRequest, dto::PlogAppendResponse>(dto::Verbs::K23SI_PERSISTENT_APPEND, request, *ep, _plog_timeout()));
    }

    return seastar::when_all_succeed(appendFutures.begin(), appendFutures.end())
        .then([this, expected_offset, appended_offset](std::vector<std::tuple<Status, dto::PlogAppendResponse> >&& results) { 
            Status return_status;
            for (auto& result: results){
                auto& [status, response] = result;
                return_status = std::move(status);
                if (!return_status.is2xxOK()) 
                    break;
                if (response.offset != expected_offset || response.bytes_appended != appended_offset){
                    return_status = Statuses::S500_Internal_Server_Error("offset inconsistent");
                    break;
                }
            }
            return seastar::make_ready_future<std::tuple<Status, uint32_t> >(std::tuple<Status, uint32_t>(std::move(return_status), std::move(expected_offset)));
        });
}


seastar::future<std::tuple<Status, Payload>> PlogClient::read(String plogId, uint32_t offset, uint32_t size){
    dto::PlogReadRequest request{.plogId = std::move(plogId), .offset=offset, .size=size};

    return RPC().callRPC<dto::PlogReadRequest, dto::PlogReadResponse>(dto::Verbs::K23SI_PERSISTENT_READ, request, *_partitionMapEndpoints[_partitionNameList[_partition_map_pointer]][0], _plog_timeout()).
        then([this] (auto&& result) {
            auto& [status, response] = result;

            return seastar::make_ready_future<std::tuple<Status, Payload> >(std::tuple<Status, Payload>(std::move(status), std::move(response.payload)));
        });
}

seastar::future<std::tuple<Status, uint32_t>> PlogClient::seal(String plogId, uint32_t offset){
    dto::PlogSealRequest request{.plogId = std::move(plogId), .offset=offset};

    std::vector<seastar::future<std::tuple<Status, dto::PlogSealResponse> > > sealFutures;
    for (auto& ep:_partitionMapEndpoints[_partitionNameList[_partition_map_pointer]]){
        sealFutures.push_back(RPC().callRPC<dto::PlogSealRequest, dto::PlogSealResponse>(dto::Verbs::K23SI_PERSISTENT_SEAL, request, *ep, _plog_timeout()));
    }

    return seastar::when_all_succeed(sealFutures.begin(), sealFutures.end())
        .then([this](std::vector<std::tuple<Status, dto::PlogSealResponse> >&& results) { 
            Status return_status;
            uint32_t sealed_offset;
            for (auto& result: results){
                auto& [status, response] = result;
                return_status = std::move(status);
                sealed_offset = response.offset;
                if (!return_status.is2xxOK()) 
                    break;
            }
            return seastar::make_ready_future<std::tuple<Status, uint32_t> >(std::tuple<Status, uint32_t>(std::move(return_status), std::move(sealed_offset)));
        });
}

String PlogClient::generate_plogId(){
    String plogid = "TPCC_CLIENT_plog_0123456789";
    std::mt19937 g(std::rand());
    std::shuffle(plogid.begin()+18, plogid.end(), g);
    return plogid;
}

bool PlogClient::selectPartitionGroup(String name){
    auto iter = _partitionNameMap.find(name);
    if (iter == _partitionNameMap.end()) {
        return false;
    }
    _partition_map_pointer = iter->second;
    return true;
}


} // k2
