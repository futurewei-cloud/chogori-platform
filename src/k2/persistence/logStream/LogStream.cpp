/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include "LogStream.h"
#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/Collection.h>
#include <k2/dto/Persistence.h>
#include <k2/dto/LogStream.h>
#include <k2/transport/Status.h>
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

LogStreamBase::LogStreamBase() {
    K2LOG_D(log::lgbase, "ctor");
}

LogStreamBase::~LogStreamBase() {
    K2LOG_D(log::lgbase, "ctor");
}

seastar::future<> 
LogStreamBase::_initPlogClient(String persistenceClusterName, String cpoUrl){
    return _client.init(persistenceClusterName, cpoUrl);
}

seastar::future<Status> 
LogStreamBase::_preallocatePlogs(){
    K2LOG_D(log::lgbase, "Pre Allocated Plogs for LogStreaBase");
    if (_initialized){
        return seastar::make_ready_future<Status>(Statuses::S409_Conflict("Log stream already created"));
    }
    _switchingInProgress = false;

    // create metadata plogs in advance
    std::vector<seastar::future<std::tuple<Status, String> > > createFutures;
    for (uint32_t i = 0; i < PLOG_POOL_SIZE + 1; ++i){
        createFutures.push_back(_client.create());
    }
    return seastar::when_all_succeed(createFutures.begin(), createFutures.end())
    .then([this] (auto&& responses){
        for (auto& response: responses){
            auto& [status, plogId] = response;
            if (!status.is2xxOK()){
                return seastar::make_ready_future<Status>(std::move(status));
            }
            _preallocatedPlogPool.push_back(std::move(plogId));
        }
        return seastar::make_ready_future<Status>(Statuses::S201_Created(""));
    });
}


seastar::future<Status>
LogStreamBase::_activeAndPersistTheFirstPlog(){
    String plogId = _preallocatedPlogPool.back();
    _preallocatedPlogPool.pop_back();
    PlogInfo info{};
    _firstPlogId = plogId;
    _currentPlogId = plogId;
    _usedPlogInfo[std::move(plogId)] = std::move(info);
    
    // persist this used plog info
    return _addNewPlog(0, _currentPlogId)
    .then([this] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        _initialized = true;
        return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
    });
}


seastar::future<std::tuple<Status, dto::AppendResponse> > 
LogStreamBase::append(dto::AppendWithIdAndOffsetRequest request){
    if (!_initialized){
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S503_Service_Unavailable("LogStream has not been intialized"), dto::AppendResponse{}));
    }
    if (_currentPlogId != request.plogId || _usedPlogInfo[_currentPlogId].sealed || _usedPlogInfo[_currentPlogId].currentOffset != request.offset){
        if (_currentPlogId != request.plogId){
            K2LOG_W(log::lgbase, "LogStreamBase append request plog Id inconsistent, expected plod Id: {}, received plog Id: {}", _currentPlogId, request.plogId);
        }
        if (_usedPlogInfo[_currentPlogId].sealed){
            K2LOG_W(log::lgbase, "LogStreamBase append request error: plog Id:{} has been sealed", _currentPlogId);
        }
        if (_usedPlogInfo[_currentPlogId].currentOffset != request.offset){
            K2LOG_W(log::lgbase, "LogStreamBase append request offset inconsistent, expected offset: {}, received offset: {}", _usedPlogInfo[_currentPlogId].currentOffset, request.offset);
        }
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S403_Forbidden("LogStreamBase append request information inconsistent"), dto::AppendResponse{}));
    }

    return append(dto::AppendRequest{.payload=std::move(request.payload)});
};


seastar::future<std::tuple<Status, dto::AppendResponse> > 
LogStreamBase::append(dto::AppendRequest request){
    if (!_initialized){
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S503_Service_Unavailable("LogStream has not been intialized"), dto::AppendResponse{}));
    }
    String plogId = _currentPlogId;

    if (_usedPlogInfo[plogId].currentOffset + request.payload.getSize() > PLOG_MAX_SIZE){
        // switch to a new plog
        return _switchPlogAndAppend(std::move(request.payload));
    }
    else{
        //The passed-in payload is appended into current active Plog. 
        //But currently there may be a plog switch in progress(i.e. _switched is set), and if that is case, 
        // we delay the response to this append request till switch is successfully done in the future.
        uint32_t current_offset = _usedPlogInfo[plogId].currentOffset;
        _usedPlogInfo[plogId].currentOffset += request.payload.getSize();
        uint32_t expect_appended_offset = _usedPlogInfo[plogId].currentOffset;
        return _client.append(plogId, current_offset, std::move(request.payload)).
        then([this, plogId, expect_appended_offset] (auto&& response){
            auto& [status, appended_offset] = response;

            if (!status.is2xxOK() || expect_appended_offset != appended_offset) {
                return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S500_Internal_Server_Error("unable to append a plog"), dto::AppendResponse{}));
            }
            // Check weather there is a sealed request that did not receive the response
            if (_switchingInProgress){
                _switchRequestWaiters.emplace_back(seastar::promise<>());
                // if there is a flying sealed request, we should not notify the client until we receive the response of that sealed request
                return _switchRequestWaiters.back().get_future().
                then([plogId, appended_offset] (){
                    return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S201_Created("append success"), dto::AppendResponse{.plogId=std::move(plogId), .current_offset=appended_offset}));
                });
            }
            else{
                return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S201_Created("append success"), dto::AppendResponse{.plogId=std::move(plogId), .current_offset=appended_offset}));
            }
        });
    }
};

seastar::future<std::tuple<Status, dto::AppendResponse> > 
LogStreamBase::_switchPlogAndAppend(Payload payload){
    if (_preallocatedPlogPool.size() == 0){
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S507_Insufficient_Storage("no available preallocated plog"), dto::AppendResponse{}));
    }
    String sealed_plogId = _currentPlogId;
    PlogInfo& targetPlogInfo = _usedPlogInfo[sealed_plogId];
    uint32_t sealed_offest = targetPlogInfo.currentOffset;
    
    String new_plogId = _preallocatedPlogPool.back();
    _preallocatedPlogPool.pop_back();
    PlogInfo info{};
    _usedPlogInfo[new_plogId] = std::move(info);
    _usedPlogInfo[sealed_plogId].nextPlogId = new_plogId;
    
    _switchingInProgress = true;
    _usedPlogInfo[new_plogId].currentOffset += payload.getSize();
    _currentPlogId = new_plogId;
    uint32_t expect_appended_offset =  _usedPlogInfo[new_plogId].currentOffset;

    // The following process could be asynchronous 
    std::vector<seastar::future<Status>> waitFutures;
    // Append to the new Plog
    waitFutures.push_back(_client.append(new_plogId, 0, std::move(payload))
    .then([expect_appended_offset] (auto&& response){
        auto& [status, appended_offset] = response;
        if (!status.is2xxOK() || expect_appended_offset != appended_offset) {
            return seastar::make_ready_future<Status>(Statuses::S500_Internal_Server_Error("unable to append a plog"));
        }
        return seastar::make_ready_future<Status>(Statuses::S200_OK("append success"));
    }));
    // Seal the old Plog
    waitFutures.push_back(_client.seal(sealed_plogId, sealed_offest)
    .then([this, sealed_plogId] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(Statuses::S500_Internal_Server_Error("unable to seal a plog"));
        }
        _usedPlogInfo[sealed_plogId].sealed = true;
        return seastar::make_ready_future<Status>(Statuses::S200_OK("seal success"));
    }));
    // Preallocated a new plog
    waitFutures.push_back(_client.create()
    .then([this] (auto&& response){
        auto& [status, plogId] = response;
        if (!status.is2xxOK()){
            return seastar::make_ready_future<Status>(Statuses::S500_Internal_Server_Error("unable to create plog for Logstream Base"));
        }
        _preallocatedPlogPool.push_back(std::move(plogId));
        return seastar::make_ready_future<Status>(Statuses::S200_OK("create plog success"));
    }));
    // Persist the metadata of sealed Plog's Offset and new PlogId
    waitFutures.push_back(_addNewPlog(sealed_offest, new_plogId)
    .then([this] (auto&& response){
        auto& status = response;
        if (!status.is2xxOK()){
            return seastar::make_ready_future<Status>(Statuses::S500_Internal_Server_Error("unable to persist metadata"));
        }
        return seastar::make_ready_future<Status>(Statuses::S200_OK("persist metadata success"));
    }));

    // Clear the switchRequestWaiters
    return seastar::when_all_succeed(waitFutures.begin(), waitFutures.end())
    .then([this, new_plogId, expect_appended_offset] (auto&& responses){
        _switchingInProgress = false;
        // send all the pending response to client 
        for (auto& request: _switchRequestWaiters){
            request.set_value();
        }
        _switchRequestWaiters.clear();

        for (auto& status: responses){
            if (!status.is2xxOK()){
                return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(std::move(status), dto::AppendResponse{}));
            }
        }
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S200_OK("append success"), dto::AppendResponse{.plogId=std::move(new_plogId), .current_offset=expect_appended_offset}));
    });
}


seastar::future<std::tuple<Status, dto::ReadResponse> > 
LogStreamBase::read(dto::ReadWithTokenRequest request){
    return read(dto::ReadRequest{.start_plogId = request.token.plogId, .start_offset=request.token.offset, .size=request.size});
}


seastar::future<std::tuple<Status, dto::ReadResponse> > 
LogStreamBase::read(dto::ReadRequest request){
    auto it = _usedPlogInfo.find(request.start_plogId);
    dto::ContinuationToken token;

    if (it == _usedPlogInfo.end()) {
        return seastar::make_ready_future<std::tuple<Status, dto::ReadResponse> >(std::tuple<Status, dto::ReadResponse>(Statuses::S404_Not_Found("unable to find start plogId"), dto::ReadResponse{}));
    }

    if (PLOG_MAX_READ_SIZE < request.size)
        request.size = PLOG_MAX_READ_SIZE;
    uint32_t read_size;
    
    if (request.start_offset+request.size <= it->second.currentOffset){
        read_size = request.size;
        token.plogId = request.start_plogId;
        token.offset = request.start_offset+request.size;
    }
    else{
        read_size = it->second.currentOffset - request.start_offset;
        token.plogId = it->second.nextPlogId;
        token.offset = 0;
    }
    return _client.read(request.start_plogId, request.start_offset, read_size)
    .then([this, token] (auto&& response){
        auto& [status, payload] = response;
        if (!status.is2xxOK()){
            return seastar::make_ready_future<std::tuple<Status, dto::ReadResponse> >(std::tuple<Status, dto::ReadResponse>(std::move(status), dto::ReadResponse{}));
        }
        return seastar::make_ready_future<std::tuple<Status, dto::ReadResponse> >(std::tuple<Status, dto::ReadResponse>(std::move(status), dto::ReadResponse{.token=std::move(token), .payload=std::move(payload)}));
    });
}


seastar::future<Status> 
LogStreamBase::reload(std::vector<dto::PartitionMetdataRecord> plogsOfTheStream){
    K2LOG_D(log::lgbase, "LogStreamBase Reload");
    if (_initialized){
        return seastar::make_ready_future<Status>(Statuses::S409_Conflict("Log stream already created"));
    }
    _switchingInProgress = false;
    
    String previous_plogId = "";
    for (auto& record: plogsOfTheStream){
        PlogInfo info{.currentOffset=record.sealed_offset, .sealed=true, .nextPlogId=""};
        _usedPlogInfo[record.plogId] = std::move(info);
        if (previous_plogId == ""){
            _firstPlogId = record.plogId;
        }
        else{
            _usedPlogInfo[previous_plogId].nextPlogId = record.plogId;
        }
        _currentPlogId = record.plogId;
        
        previous_plogId = std::move(record.plogId);
    }
    _usedPlogInfo[_currentPlogId].sealed=false;

    return _preallocatePlogs().
    then([this] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        _initialized = true;
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully reloaded metadata"));
    });
}

seastar::future<std::tuple<Status, std::tuple<uint32_t, bool>>> 
LogStreamBase::getPlogStatus(String plogId){
    return _client.getPlogStatus(std::move(plogId));
}
LogStream::LogStream() {
    K2LOG_D(log::lgbase, "ctor");
}

LogStream::~LogStream() {
    K2LOG_D(log::lgbase, "ctor");
}

seastar::future<Status> 
LogStream::init(Verb name, MetadataMgr* metadataMgr, String cpoUrl, String persistenceClusterName, bool reload){
    _name = name;
    _metadataMgr = metadataMgr;
    return _initPlogClient(persistenceClusterName, cpoUrl)
    .then([this] (){
        return _preallocatePlogs();
    })
    .then([this, reload] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        if (!reload){
            return _activeAndPersistTheFirstPlog();
        }
        else{
            return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
        }
    });
}

seastar::future<Status>
LogStream::_addNewPlog(uint32_t sealedOffset, String newPlogId){
    return _metadataMgr->addNewPLogIntoLogStream(_name, sealedOffset, std::move(newPlogId));
}


MetadataMgr::MetadataMgr() {
    K2LOG_D(log::lgbase, "ctor");
}

MetadataMgr::~MetadataMgr() {
    K2LOG_D(log::lgbase, "ctor");
}

seastar::future<Status> 
MetadataMgr::init(String cpoUrl, String partitionName, String persistenceClusterName, bool reload){
    _cpo = CPOClient(cpoUrl);
    _partitionName = std::move(partitionName);
    return _initPlogClient(persistenceClusterName, cpoUrl)
    .then([this] (){
        return _preallocatePlogs();
    })
    .then([this, reload] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        if (!reload){
            return _activeAndPersistTheFirstPlog();
        }
        else{
            return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
        }
    })
    .then([this, cpoUrl, persistenceClusterName, reload] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        std::vector<seastar::future<Status> > initFutures;
        for (int logstreamName=LogStreamType::LogStreamTypeHead+1; logstreamName<LogStreamType::LogStreamTypeEnd; ++logstreamName){
            LogStream* _logstream = new LogStream();
            _logStreamMap[logstreamName] = _logstream;
            initFutures.push_back(_logStreamMap[logstreamName]->init(logstreamName, this, cpoUrl, persistenceClusterName, reload));
        }
        return seastar::when_all_succeed(initFutures.begin(), initFutures.end()).
        then([this] (auto&& responses){
            for (auto& status: responses){
                if (!status.is2xxOK()) {
                    return seastar::make_ready_future<Status>(std::move(status));
                }
            }
            return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
        });
    });
}

seastar::future<Status>
MetadataMgr::_addNewPlog(uint32_t sealedOffset, String newPlogId){
    return _cpo.PutPartitionMetadata(Deadline<>(_cpo_timeout()), _partitionName, sealedOffset, std::move(newPlogId)).
    then([this] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully persist metadata"));
    });
}

LogStream* MetadataMgr::obtainLogStream(Verb log_stream_name){
    auto it = _logStreamMap.find(log_stream_name);
    if (it == _logStreamMap.end()) {
        throw k2::dto::LogStreamRetrieveError("unable to retrieve the target logstream");
    }
    return it->second;
}


seastar::future<Status> 
MetadataMgr::addNewPLogIntoLogStream(Verb name, uint32_t sealed_offset, String new_plogId){
    Payload temp_payload(Payload::DefaultAllocator);
    temp_payload.write(name);
    temp_payload.write(sealed_offset);
    temp_payload.write(std::move(new_plogId));
    return append(dto::AppendRequest{.payload=std::move(temp_payload)})
    .then([this] (auto&& response){
        auto& [status, append_response] = response;
        K2LOG_D(log::lgbase, "{}, {}", append_response.plogId, append_response.current_offset);
        return seastar::make_ready_future<Status>(std::move(status));
    });
}


seastar::future<Status> 
MetadataMgr::replay(String cpoUrl, String partitionName, String persistenceClusterName){
    return init(cpoUrl, partitionName, persistenceClusterName, true).
    then([&] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        return _cpo.GetPartitionMetadata(Deadline<>(_cpo_timeout()), _partitionName)
        .then([&] (auto&& response){
            auto& [status, resp] = response;
            if (!status.is2xxOK()) {
                return seastar::make_ready_future<Status>(std::move(status));
            }
            return seastar::do_with(std::move(resp.records), [&] (auto& records){
                return getPlogStatus(records.back().plogId)
                .then([&] (auto&& response){
                    auto& [status, resp] = response;
                    if (!status.is2xxOK()) {
                        return seastar::make_ready_future<Status>(std::move(status));
                    }
                    records.back().sealed_offset = std::get<0>(resp);
                    return reload(records);
                })
                .then([&] (auto&& response){
                    auto& status = response;
                    if (!status.is2xxOK()) {
                        return seastar::make_ready_future<Status>(std::move(status));
                    }
                    uint32_t read_size = 0;
                    for (auto& record: records){
                        read_size += record.sealed_offset;
                    }
                    return _readMetadataPlogs(std::move(records), read_size);
                });
            });
        });
    });
}

seastar::future<Status> 
MetadataMgr::_readMetadataPlogs(std::vector<dto::PartitionMetdataRecord> records, uint32_t read_size){
    return read(dto::ReadRequest{.start_plogId=records[0].plogId, .start_offset=0, .size=read_size})
    .then([&, read_size] (auto&& response){
        uint32_t request_size = read_size;
        Payload read_payload;
        Status return_status = Statuses::S200_OK("");
        dto::ContinuationToken continuation_token;
        auto& [status, read_response] = response;
        continuation_token = std::move(read_response.token);
        
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<std::tuple<Status, Payload> >(std::tuple<Status, Payload>(std::move(status), std::move(read_payload)));
        }
        request_size -= read_response.payload.getSize();
        
        for (auto& b: read_response.payload.shareAll().release()) {
            read_payload.appendBinary(std::move(b));
        }
        
        return seastar::do_with(std::move(read_payload), std::move(request_size), std::move(continuation_token), std::move(return_status), [this] (auto& read_payload, auto& request_size, auto& continuation_token, auto& return_status){
            return seastar::do_until(
                [&] { return request_size == 0; },
                [&] {
                    
                    return read(dto::ReadWithTokenRequest{.token=continuation_token, .size=request_size})
                    .then([&] (auto&& response){
                        auto& [status, read_response] = response;
                        return_status = std::move(status);
                        if (!return_status.is2xxOK()) {
                            request_size = 0;
                            return seastar::make_ready_future<>();
                        }
                        request_size -= read_response.payload.getSize();
                        continuation_token = std::move(read_response.token);
                        for (auto& b: read_response.payload.shareAll().release()) {
                            read_payload.appendBinary( std::move(b));
                        }
                        return seastar::make_ready_future<>();
                    });
                }
            )
            .then([&] (){
                return seastar::make_ready_future<std::tuple<Status, Payload> >(std::tuple<Status, Payload>(std::move(return_status), std::move(read_payload)));
            });
        });
    })
    .then([&] (auto&& response){
        auto& [status, payload] = response;
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        return _reloadLogStreams(std::move(payload));
    });
}

seastar::future<Status> 
MetadataMgr::_reloadLogStreams(Payload payload){
    std::unordered_map<Verb, std::vector<dto::PartitionMetdataRecord> > logStreamRecords;
    Verb logStreamName;
    uint32_t offset;
    String plogId;
    payload.seek(0);
    while (payload.getDataRemaining() > 0){
        payload.read(logStreamName);
        payload.read(offset);
        payload.read(plogId);
        auto it = logStreamRecords.find(logStreamName);
        if (it == logStreamRecords.end()) {
            std::vector<dto::PartitionMetdataRecord> metadataRecords;
            dto::PartitionMetdataRecord element{.plogId=plogId, .sealed_offset=0};
            metadataRecords.push_back(std::move(element));
            logStreamRecords[logStreamName] = std::move(metadataRecords);
        }
        else{
            it->second.back().sealed_offset = offset;
            dto::PartitionMetdataRecord element{.plogId=plogId, .sealed_offset=0};
            it->second.push_back(std::move(element));
        }
    }
    
    return seastar::do_with(std::move(logStreamRecords), [&] (auto& logStreamRecords){
        std::vector<seastar::future<std::tuple<Status, std::tuple<uint32_t, bool> > > > getStatusFutures;
        for (int logstreamName=LogStreamType::LogStreamTypeHead+1; logstreamName<LogStreamType::LogStreamTypeEnd; ++logstreamName){
            getStatusFutures.push_back(_logStreamMap[logstreamName]->getPlogStatus(logStreamRecords[logstreamName].back().plogId));
        }
        return seastar::when_all_succeed(getStatusFutures.begin(), getStatusFutures.end())
        .then([&] (auto&& responses){
            Verb logstreamName=LogStreamType::LogStreamTypeHead;
            std::vector<seastar::future<Status> > reloadFutures;
            for (auto& response: responses){
                auto& [status, resp] = response;
                if (!status.is2xxOK()) {
                    return seastar::make_ready_future<std::vector<Status> >(std::vector{std::move(status)});
                }
                ++logstreamName;
                logStreamRecords[logstreamName].back().sealed_offset = std::get<0>(resp);

                reloadFutures.push_back(_logStreamMap[logstreamName]->reload(std::move(logStreamRecords[logstreamName])));
            }
            return seastar::when_all_succeed(reloadFutures.begin(), reloadFutures.end());
        })
        .then([this] (auto&& responses){
            for (auto& status: responses){
                if (!status.is2xxOK()) {
                    return seastar::make_ready_future<Status>(std::move(status));
                }
            }
            return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully replaied the metadata manager"));
        });
    });
}

} // k2