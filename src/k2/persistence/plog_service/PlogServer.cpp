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

#include "PlogServer.h"
#include <k2/transport/PayloadSerialization.h>
#include <seastar/core/sharded.hh>
#include <k2/transport/Payload.h>
#include <k2/transport/Status.h>
#include <k2/dto/Persistence.h>
#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>


namespace k2 {

PlogServer::PlogServer() {
    K2INFO("ctor");
}

PlogServer::~PlogServer() {
    K2INFO("dtor");
}

seastar::future<> PlogServer::gracefulStop() {
    K2INFO("stop");
    _plogMap.clear();
    return seastar::make_ready_future<>();
}

seastar::future<> PlogServer::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<dto::PlogCreateRequest, dto::PlogCreateResponse>(dto::Verbs::PERSISTENT_CREATE, [this](dto::PlogCreateRequest&& request) {
        return _handleCreate(std::move(request));
    });

    RPC().registerRPCObserver<dto::PlogAppendRequest, dto::PlogAppendResponse>(dto::Verbs::PERSISTENT_APPEND, [this](dto::PlogAppendRequest&& request) {
        return _handleAppend(std::move(request));
    });

    RPC().registerRPCObserver<dto::PlogReadRequest, dto::PlogReadResponse>(dto::Verbs::PERSISTENT_READ, [this](dto::PlogReadRequest&& request) {
        return _handleRead(std::move(request));
    });

    RPC().registerRPCObserver<dto::PlogSealRequest, dto::PlogSealResponse>(dto::Verbs::PERSISTENT_SEAL, [this](dto::PlogSealRequest&& request) {
        return _handleSeal(std::move(request));
    });

    RPC().registerRPCObserver<dto::PlogStatusRequest, dto::PlogStatusResponse>(dto::Verbs::PERSISTENT_STATUS, [this](dto::PlogStatusRequest&& request) {
        return _handleGetStatus(std::move(request));
    });
    _plogMap.clear();

    return seastar::make_ready_future<>();
}

seastar::future<std::tuple<Status, dto::PlogCreateResponse>>
PlogServer::_handleCreate(dto::PlogCreateRequest&& request){
    K2DEBUG("Received create request for " << request.plogId);
    auto iter = _plogMap.find(request.plogId);
    if (iter != _plogMap.end()) {
        return RPCResponse(Statuses::S409_Conflict("plog id already exists"), dto::PlogCreateResponse());
    }
    _plogMap.insert(std::pair<String,InternalPlog >(std::move(request.plogId), InternalPlog()));
    return RPCResponse(Statuses::S201_Created("plog created"), dto::PlogCreateResponse());
};

seastar::future<std::tuple<Status, dto::PlogAppendResponse>>
PlogServer::_handleAppend(dto::PlogAppendRequest&& request){
    auto iter = _plogMap.find(request.plogId);
    if (iter == _plogMap.end()) {
        return RPCResponse(Statuses::S404_Not_Found("plog does not exist"), dto::PlogAppendResponse());
    }
    if (iter->second.sealed){
         return RPCResponse(Statuses::S409_Conflict("plog is sealed"), dto::PlogAppendResponse());
    }
    if (iter->second.offset != request.offset){
        return RPCResponse(Statuses::S403_Forbidden("offset inconsistent"), dto::PlogAppendResponse());
    }
    if (iter->second.offset + request.payload.getSize() > PLOG_MAX_SIZE){
         return RPCResponse(Statuses::S413_Payload_Too_Large("exceeds pLog limit"), dto::PlogAppendResponse());
    }

    dto::PlogAppendResponse response;
    response.newOffset = iter->second.offset + request.payload.getSize();

    iter->second.offset += request.payload.getSize();
    // We want to use copy in order to prevent memory fragmentation. If we use shareAll() instead of copy, the payload we obtained will occupy a entire 8K block
    iter->second.payload.copyFromPayload(request.payload, request.payload.getSize());
    
    return RPCResponse(Statuses::S200_OK("append scuccess"), std::move(response));
};


seastar::future<std::tuple<Status, dto::PlogReadResponse>>
PlogServer::_handleRead(dto::PlogReadRequest&& request){
    K2DEBUG("Received read request for " << request.plogId << " with offset " << request.offset);
    auto iter = _plogMap.find(request.plogId);
    if (iter == _plogMap.end()) {
        return RPCResponse(Statuses::S404_Not_Found("plog does not exist"), dto::PlogReadResponse());
    }
    if (iter->second.offset < request.offset + request.size){
         return RPCResponse(Statuses::S413_Payload_Too_Large("exceed the maximun length"), dto::PlogReadResponse());
    }
    
    dto::PlogReadResponse response{.payload=iter->second.payload.shareRegion(request.offset, request.size)};
    return RPCResponse(Statuses::S200_OK("read success"), std::move(response));
};


seastar::future<std::tuple<Status, dto::PlogSealResponse>>
PlogServer::_handleSeal(dto::PlogSealRequest&& request){
    K2DEBUG("Received seal request for " << request.plogId);
    dto::PlogSealResponse response;
    auto iter = _plogMap.find(request.plogId);
    if (iter == _plogMap.end()) {
        return RPCResponse(Statuses::S404_Not_Found("plog does not exist"), std::move(response));
    }
    if (iter->second.sealed){
        response.sealedOffset = iter->second.offset;
        if (request.truncateOffset == iter->second.offset){
            return RPCResponse(Statuses::S200_OK("sealed success"), std::move(response));
        }
        else{
            return RPCResponse(Statuses::S409_Conflict("plog already sealed"), std::move(response));
        }
    }

    iter->second.sealed = true;
    if (iter->second.offset < request.truncateOffset){
        response.sealedOffset = iter->second.offset;
        return RPCResponse(Statuses::S200_OK("sealed offset inconsistent"), std::move(response));
    }

    _plogMap[request.plogId].offset = request.truncateOffset;
    _plogMap[request.plogId].payload.seek(request.truncateOffset);
    _plogMap[request.plogId].payload.truncateToCurrent();

    response.sealedOffset = request.truncateOffset;
    return RPCResponse(Statuses::S200_OK("sealed success"), std::move(response));
};

seastar::future<std::tuple<Status, dto::PlogStatusResponse>>
PlogServer::_handleGetStatus(dto::PlogStatusRequest&& request){
    auto iter = _plogMap.find(request.plogId);
    if (iter == _plogMap.end()) {
        return RPCResponse(Statuses::S404_Not_Found("plog does not exist"), dto::PlogStatusResponse());
    }
    
    dto::PlogStatusResponse response{.currentOffset=iter->second.offset, .sealed=iter->second.sealed};
    return RPCResponse(Statuses::S200_OK("read success"), std::move(response));
};


} // namespace k2
