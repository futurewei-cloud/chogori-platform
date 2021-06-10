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
LogStreamBase::_initPlogClient(String persistenceClusterName){
    return _client.init(persistenceClusterName);
}

seastar::future<Status>
LogStreamBase::_preallocatePlog(){
    K2LOG_D(log::lgbase, "Pre Allocated Plogs for LogStreaBase");
    if (_initialized){
        return seastar::make_ready_future<Status>(Statuses::S409_Conflict("Log stream already created"));
    }
    _switchingInProgress = false;

    // create preallocate plog
    return _client.create()
    .then([this] (auto&& response){
        auto& [status, plogId] = response;
        if (!status.is2xxOK()){
            return seastar::make_ready_future<Status>(std::move(status));
        }
        _preallocatedPlogId = plogId;
        return seastar::make_ready_future<Status>(Statuses::S201_Created(""));
    });
}


seastar::future<Status>
LogStreamBase::_activeAndPersistTheFirstPlog(){
    return _client.create()
    .then([this] (auto&& response){
        auto& [status, plogId] = response;
        if (!status.is2xxOK()){
            return seastar::make_ready_future<Status>(std::move(status));
        }
        _PlogInfo info{};
        _firstPlogId = plogId;
        _currentPlogId = plogId;
        _usedPlogInfo[std::move(plogId)] = std::move(info);
        return _addNewPlog(0, _currentPlogId);
    })
    .then([this] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        _initialized = true;
        return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
    });
}


seastar::future<std::tuple<Status, dto::AppendResponse> >
LogStreamBase::append_data_to_plogs(dto::AppendWithIdAndOffsetRequest request){
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

    return append_data_to_plogs(dto::AppendRequest{.payload=std::move(request.payload)});
};


seastar::future<std::tuple<Status, dto::AppendResponse> >
LogStreamBase::append_data_to_plogs(dto::AppendRequest request){
    if (!_initialized){
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S503_Service_Unavailable("LogStream has not been intialized"), dto::AppendResponse{}));
    }
    String plogId = _currentPlogId;
    _PlogInfo& currentPlogInfo = _usedPlogInfo[plogId];
    if (currentPlogInfo.currentOffset + request.payload.getSize() > PLOG_MAX_SIZE){
        // switch to a new plog, and append the data
        return _switchPlogAndAppend(std::move(request.payload));
    }
    else{
        //The passed-in payload is appended into current active Plog.
        //But currently there may be a plog switch in progress(i.e. _switched is set), and if that is case,
        // we delay the response to this append request till switch is successfully done in the future.
        uint32_t current_offset = currentPlogInfo.currentOffset;
        currentPlogInfo.currentOffset += request.payload.getSize();
        uint32_t expect_appended_offset = currentPlogInfo.currentOffset;

        return _client.append(dto::PlogAppendRequest{.plogId=plogId, .offset=current_offset, .payload=std::move(request.payload)}).
        then([this, plogId, current_offset, expect_appended_offset] (auto&& response){
            auto& [status, return_response] = response;

            uint32_t appended_offset = return_response.newOffset;
            // If this append failed, we will switch to a new plog and append the data
            if (!status.is2xxOK() || expect_appended_offset != appended_offset) {
                _usedPlogInfo[plogId].currentOffset = current_offset;
                return _switchPlogAndAppend(std::move(return_response.return_payload));
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
    if (_preallocatedPlogId == ""){
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S507_Insufficient_Storage("no available preallocated plog"), dto::AppendResponse{}));
    }
    String sealed_plogId = _currentPlogId;
    _PlogInfo& targetPlogInfo = _usedPlogInfo[sealed_plogId];
    uint32_t sealed_offest = targetPlogInfo.currentOffset;

    String new_plogId = _preallocatedPlogId;
    _preallocatedPlogId = "";
    _PlogInfo info{};
    _usedPlogInfo[new_plogId] = std::move(info);
    _usedPlogInfo[sealed_plogId].nextPlogId = new_plogId;

    _switchingInProgress = true;
    _usedPlogInfo[new_plogId].currentOffset += payload.getSize();
    _currentPlogId = new_plogId;
    uint32_t expect_appended_offset =  _usedPlogInfo[new_plogId].currentOffset;

    // The following process could be asynchronous
    std::vector<seastar::future<Status>> waitFutures;
    // Append to the new Plog
    waitFutures.push_back(_client.append(dto::PlogAppendRequest{.plogId=new_plogId, .offset=0, .payload=std::move(payload)})
    .then([expect_appended_offset] (auto&& response){
        auto& [status, return_response] = response;
        if (!status.is2xxOK() || expect_appended_offset != return_response.newOffset) {
            return seastar::make_ready_future<Status>(Statuses::S500_Internal_Server_Error("unable to append a plog"));
        }
        return seastar::make_ready_future<Status>(Statuses::S201_Created("append success"));
    }));
    // Seal the old Plog

    waitFutures.push_back(_client.seal(dto::PlogSealRequest{.plogId=sealed_plogId, .truncateOffset=sealed_offest})
    .then([this, sealed_plogId] (auto&& response){
        auto& [status, return_response] = response;
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
        _preallocatedPlogId = plogId;
        return seastar::make_ready_future<Status>(Statuses::S201_Created("create plog success"));
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
        return seastar::make_ready_future<std::tuple<Status, dto::AppendResponse> >(std::tuple<Status, dto::AppendResponse>(Statuses::S201_Created("append success"), dto::AppendResponse{.plogId=std::move(new_plogId), .current_offset=expect_appended_offset}));
    });
}


seastar::future<std::tuple<Status, dto::ReadResponse> >
LogStreamBase::read_data_from_plogs(dto::ReadWithTokenRequest request){
    return read_data_from_plogs(dto::ReadRequest{.start_plogId = request.token.plogId, .start_offset=request.token.offset, .size=request.size});
}


seastar::future<std::tuple<Status, dto::ReadResponse> >
LogStreamBase::read_data_from_plogs(dto::ReadRequest request){
    auto it = _usedPlogInfo.find(request.start_plogId);
    dto::LogStreamReadContinuationToken token;

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

    return _client.read(dto::PlogReadRequest{.plogId=request.start_plogId, .offset=request.start_offset, .size=read_size})
    .then([this, token] (auto&& response){
        auto& [status, return_response] = response;
        if (!status.is2xxOK()){
            return seastar::make_ready_future<std::tuple<Status, dto::ReadResponse> >(std::tuple<Status, dto::ReadResponse>(std::move(status), dto::ReadResponse{}));
        }
        return seastar::make_ready_future<std::tuple<Status, dto::ReadResponse> >(std::tuple<Status, dto::ReadResponse>(std::move(status), dto::ReadResponse{.token=std::move(token), .payload=std::move(return_response.payload)}));
    });
}


seastar::future<Status>
LogStreamBase::_reload(std::vector<dto::PartitionMetdataRecord> plogsOfTheStream){
    K2LOG_D(log::lgbase, "LogStreamBase Reload");
    if (_initialized){
        return seastar::make_ready_future<Status>(Statuses::S409_Conflict("Log stream already created"));
    }
    _switchingInProgress = false;

    String previous_plogId = "";
    for (auto& record: plogsOfTheStream){
        _PlogInfo info{.currentOffset=record.sealed_offset, .sealed=true, .nextPlogId=""};
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

    return _preallocatePlog().
    then([this] (auto&& status){
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        _initialized = true;
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully reloaded metadata"));
    });
}

seastar::future<std::tuple<Status, dto::PlogGetStatusResponse>>
LogStreamBase::_getPlogStatus(dto::PlogGetStatusRequest request){
    return _client.getPlogStatus(std::move(request));
}
LogStream::LogStream() {
    K2LOG_D(log::lgbase, "ctor");
}

LogStream::~LogStream() {
    K2LOG_D(log::lgbase, "ctor");
}

seastar::future<Status>
LogStream::init(LogStreamType name, std::shared_ptr<PartitionMetadataMgr> metadataMgr, String persistenceClusterName, bool reload){
    _name = name;
    _metadataMgr = metadataMgr;
    return _initPlogClient(persistenceClusterName)
    .then([this] (){
        return _preallocatePlog();
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


PartitionMetadataMgr::PartitionMetadataMgr() {
    K2LOG_D(log::lgbase, "ctor");
}

PartitionMetadataMgr::~PartitionMetadataMgr() {
    K2LOG_D(log::lgbase, "ctor");
}

seastar::future<Status>
PartitionMetadataMgr::init(String cpoUrl, String partitionName, String persistenceClusterName){
    _cpo.init(cpoUrl);
    _partitionName = std::move(partitionName);
    return _cpo.GetPartitionMetadata(Deadline<>(_cpo_timeout()), _partitionName)
    .then([&, cpoUrl, persistenceClusterName] (auto&& response){
        auto& [status, resp] = response;
        bool reload;
        if (status.is2xxOK()) {
            reload = true;
        }
        else{
            if (status == Statuses::S404_Not_Found){
                reload = false;
            }
            else{
                return seastar::make_ready_future<Status>(std::move(status));
            }
        }

        return seastar::do_with(std::move(resp.records), std::move(reload), std::move(cpoUrl), std::move(persistenceClusterName), [&] (auto& records, auto& reload, auto&& cpoUrl, auto&&persistenceClusterName){
            return _initPlogClient(persistenceClusterName)
            .then([this] (){
                return _preallocatePlog();
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
            .then([this, cpoUrl, persistenceClusterName, reload, records] (auto&& status){
                if (!status.is2xxOK()) {
                    return seastar::make_ready_future<Status>(std::move(status));
                }
                std::vector<seastar::future<Status> > initFutures;
                LogStreamType logstreamName;
                for (auto& name:LogStreamTypeNames){
                    logstreamName = LogStreamTypeFromStr(name);
                    std::shared_ptr<LogStream> _logstream = std::make_shared<LogStream>();
                    _logStreamMap[logstreamName] = _logstream;
                    initFutures.push_back(_logStreamMap[logstreamName]->init(logstreamName, shared_from_this(), persistenceClusterName, reload));
                }
                return seastar::when_all_succeed(initFutures.begin(), initFutures.end()).
                then([this, reload, records] (auto&& responses){
                    for (auto& status: responses){
                        if (!status.is2xxOK()) {
                            return seastar::make_ready_future<Status>(std::move(status));
                        }
                    }
                    if (!reload){
                        return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
                    }
                    else{
                        return _replay(std::move(records));
                    }
                });
            });
        });
    });
}

seastar::future<Status>
PartitionMetadataMgr::_addNewPlog(uint32_t sealedOffset, String newPlogId){
    return _cpo.PutPartitionMetadata(Deadline<>(_cpo_timeout()), _partitionName, sealedOffset, std::move(newPlogId)).
    then([this] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            return seastar::make_ready_future<Status>(std::move(status));
        }
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully persist metadata"));
    });
}

std::tuple<Status, std::shared_ptr<LogStream>> PartitionMetadataMgr::obtainLogStream(LogStreamType log_stream_name){
    auto it = _logStreamMap.find(log_stream_name);
    if (it == _logStreamMap.end()) {
        return std::tuple<Status, std::shared_ptr<LogStream>>(std::tuple<Status, std::shared_ptr<LogStream>>(Statuses::S404_Not_Found("unable to retrieve the target logstream"), NULL));
    }
    return std::tuple<Status, std::shared_ptr<LogStream>>(std::tuple<Status, std::shared_ptr<LogStream>>(Statuses::S200_OK("successfully obtain logstream"), it->second));
}


seastar::future<Status>
PartitionMetadataMgr::addNewPLogIntoLogStream(LogStreamType name, uint32_t sealed_offset, String new_plogId){
    Payload temp_payload(Payload::DefaultAllocator);
    temp_payload.write(name);
    temp_payload.write(sealed_offset);
    temp_payload.write(std::move(new_plogId));
    return append_data_to_plogs(dto::AppendRequest{.payload=std::move(temp_payload)})
    .then([this] (auto&& response){
        auto& [status, append_response] = response;
        K2LOG_D(log::lgbase, "{}, {}", append_response.plogId, append_response.current_offset);
        return seastar::make_ready_future<Status>(std::move(status));
    });
}


seastar::future<Status>
PartitionMetadataMgr::_replay(std::vector<dto::PartitionMetdataRecord> records){
    return seastar::do_with(std::move(records), [&] (auto& records){
        // since this records will not contain the last plog's offset, we will retreive this information from Plog Servers
        return _getPlogStatus(dto::PlogGetStatusRequest{.plogId=records.back().plogId})
        .then([&] (auto&& response){
            auto& [status, return_response] = response;
            if (!status.is2xxOK()) {
                return seastar::make_ready_future<Status>(std::move(status));
            }
            records.back().sealed_offset = return_response.currentOffset;
            // 3. reload the _usedPlogInfo, _firstPlogId, _currentPlogId for metadata manager itself
            return _reload(records);
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
}

// 4. retrive the metadata of all the logstreams from metadata plogs
seastar::future<Status>
PartitionMetadataMgr::_readMetadataPlogs(std::vector<dto::PartitionMetdataRecord> records, uint32_t read_size){
    return read_data_from_plogs(dto::ReadRequest{.start_plogId=records[0].plogId, .start_offset=0, .size=read_size})
    .then([&, read_size] (auto&& response){
        uint32_t request_size = read_size;
        Payload read_payload;
        Status return_status = Statuses::S200_OK("");
        dto::LogStreamReadContinuationToken continuation_token;
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

                    return read_data_from_plogs(dto::ReadWithTokenRequest{.token=continuation_token, .size=request_size})
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

// 5. reload the _usedPlogInfo, _firstPlogId, _currentPlogId for all the logstreams
seastar::future<Status>
PartitionMetadataMgr::_reloadLogStreams(Payload payload){
    std::unordered_map<LogStreamType, std::vector<dto::PartitionMetdataRecord> > logStreamRecords;
    LogStreamType logStreamName;
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
        std::vector<seastar::future<std::tuple<Status, dto::PlogGetStatusResponse > > > getStatusFutures;
        LogStreamType logstreamName;
        for (auto& name:LogStreamTypeNames){
            logstreamName = LogStreamTypeFromStr(name);
            getStatusFutures.push_back(_logStreamMap[logstreamName]->_getPlogStatus(dto::PlogGetStatusRequest{.plogId=logStreamRecords[logstreamName].back().plogId}));
        }
        return seastar::when_all_succeed(getStatusFutures.begin(), getStatusFutures.end())
        .then([&] (auto&& responses){
            LogStreamType logstreamName;
            int count = 0;
            std::vector<seastar::future<Status> > reloadFutures;
            for (auto& response: responses){
                auto& [status, return_response] = response;
                if (!status.is2xxOK()) {
                    return seastar::make_ready_future<std::vector<Status> >(std::vector{std::move(status)});
                }
                logstreamName = LogStreamTypeFromStr(LogStreamTypeNames[count]);
                ++count;
                logStreamRecords[logstreamName].back().sealed_offset = return_response.currentOffset;

                reloadFutures.push_back(_logStreamMap[logstreamName]->_reload(std::move(logStreamRecords[logstreamName])));
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
