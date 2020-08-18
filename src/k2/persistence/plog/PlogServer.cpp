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
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>


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
    RPC().registerRPCObserver<dto::PlogCreateRequest, dto::PlogCreateResponse>(dto::Verbs::K23SI_PERSISTENT_CREATE, [this](dto::PlogCreateRequest&& request) {
        return handleCreate(std::move(request));
    });

    RPC().registerRPCObserver<dto::PlogAppendRequest, dto::PlogAppendResponse>(dto::Verbs::K23SI_PERSISTENT_APPEND, [this](dto::PlogAppendRequest&& request) {
        K2INFO("Received Payload size " << request.payload.getSize() << " and offest " << request.offset);
        return handleAppend(std::move(request));
    });

    RPC().registerRPCObserver<dto::PlogReadRequest, dto::PlogReadResponse>(dto::Verbs::K23SI_PERSISTENT_READ, [this](dto::PlogReadRequest&& request) {
        return handleRead(std::move(request));
    });

    RPC().registerRPCObserver<dto::PlogSealRequest, dto::PlogSealResponse>(dto::Verbs::K23SI_PERSISTENT_SEAL, [this](dto::PlogSealRequest&& request) {
        return handleSeal(std::move(request));
    });
    _plogMap.clear();

    _cpo = CPOClient(String(_cpo_url()));
    auto ep = (seastar::engine()._rdma_stack?
        k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto):
        k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto));
    K2INFO(ep->getURL());
    //return seastar::make_ready_future<>();
    return _cpo.RegisterPlogServer(Deadline<>(register_plog_server_deadline()), ep->getURL()).discard_result();
}

seastar::future<std::tuple<Status, dto::PlogCreateResponse>>
PlogServer::handleCreate(dto::PlogCreateRequest&& request){
    K2INFO("Received create request for " << request.plogId);
    auto iter = _plogMap.find(request.plogId);
    if (iter != _plogMap.end()) {
        return RPCResponse(Statuses::S400_Bad_Request("plog already exists"), dto::PlogCreateResponse());
    }
    _plogMap.insert(std::pair<String,PlogPage >(std::move(request.plogId), PlogPage()));
    return RPCResponse(Statuses::S200_OK("OK"), dto::PlogCreateResponse());
};

seastar::future<std::tuple<Status, dto::PlogAppendResponse>>
PlogServer::handleAppend(dto::PlogAppendRequest&& request){
    K2INFO("Received append request for " << request.plogId << " with size" << request.payload.getSize() << " and offset " << request.offset);
    auto iter = _plogMap.find(request.plogId);
    if (iter == _plogMap.end()) {
        return RPCResponse(Statuses::S400_Bad_Request("plog does not exist"), dto::PlogAppendResponse());
    }
    if (iter->second.sealed){
         return RPCResponse(Statuses::S400_Bad_Request("plog is sealed"), dto::PlogAppendResponse());
    }
    if (iter->second.offset != request.offset){
        K2INFO("offset inconsistent");
        return RPCResponse(Statuses::S400_Bad_Request("offset inconsistent"), dto::PlogAppendResponse());
    }
    if (iter->second.offset + request.payload.getSize() > PLOG_MAX_SIZE){
         return RPCResponse(Statuses::S400_Bad_Request("exceeds pLog limit"), dto::PlogAppendResponse());
    }

    dto::PlogAppendResponse response;
    response.offset = iter->second.offset + request.payload.getSize();
    response.bytes_appended = request.payload.getSize();
    K2INFO(response.offset << " " << response.bytes_appended);

    iter->second.offset += request.payload.getSize();
    iter->second.payload.write(request.payload.copy());
    request.payload.clear();
    
    return RPCResponse(Statuses::S200_OK("append cuccess"), std::move(response));
};


seastar::future<std::tuple<Status, dto::PlogReadResponse>>
PlogServer::handleRead(dto::PlogReadRequest&& request){
    K2INFO("Received read request for " << request.plogId);
    auto iter = _plogMap.find(request.plogId);
    if (iter == _plogMap.end()) {
        return RPCResponse(Statuses::S400_Bad_Request("plog does not exist"), dto::PlogReadResponse());
    }
    if (iter->second.offset < request.offset + request.size){
         return RPCResponse(Statuses::S400_Bad_Request("exceeds the length of plog"), dto::PlogReadResponse());
    }
    
    Binary binary(request.size);
    Payload payload;

    iter->second.payload.seek(request.offset);
    iter->second.payload.read(binary, request.size);
    payload.appendBinary(std::move(binary));

    dto::PlogReadResponse response;
    response.payload = std::move(payload);
    return RPCResponse(Statuses::S200_OK("read success"), std::move(response));
};


seastar::future<std::tuple<Status, dto::PlogSealResponse>>
PlogServer::handleSeal(dto::PlogSealRequest&& request){
    K2INFO("Received seal request for " << request.plogId);
    auto iter = _plogMap.find(request.plogId);
    if (iter == _plogMap.end()) {
        return RPCResponse(Statuses::S400_Bad_Request("plog does not exist"), dto::PlogSealResponse());
    }
    if (_plogMap[request.plogId].sealed){
        return RPCResponse(Statuses::S400_Bad_Request("plog already sealed"), dto::PlogSealResponse());
    }
    dto::PlogSealResponse response; 
    _plogMap[request.plogId].sealed = true;
    if (_plogMap[request.plogId].offset < request.offset){
        response.offset = _plogMap[request.plogId].offset;
        return RPCResponse(Statuses::S200_OK("sealed offset inconsistent"), std::move(response));
    }

    _plogMap[request.plogId].offset = request.offset;
    _plogMap[request.plogId].payload.seek(request.offset);
    _plogMap[request.plogId].payload.truncateToCurrent();

    response.offset = request.offset;
    return RPCResponse(Statuses::S200_OK("sealed success"), std::move(response));
};


} // namespace k2
