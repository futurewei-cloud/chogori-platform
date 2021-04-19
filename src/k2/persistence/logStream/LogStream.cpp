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
    K2LOG_I(log::lgbase, "dtor");
}

LogStreamBase::~LogStreamBase() {
    K2LOG_I(log::lgbase, "dtor");
}

seastar::future<> 
LogStreamBase::_init_plog_client(String cpo_url, String persistenceClusterName){
    return _client.init(persistenceClusterName, cpo_url);
}

seastar::future<> 
LogStreamBase::_preallocatePlogs(){
    K2LOG_I(log::lgbase, "Pre Allocated Plogs for LogStreaBase");
    if (_create){
        throw k2::dto::LogStreamBaseExistError("Log stream already created");
    }
    _switched = false;
    _stopped = true;

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
                throw k2::dto::PlogCreateError("unable to create plog for Logstream Base");
            }
            _preallocatedPlogPool.push_back(std::move(plogId));
        }
        return seastar::make_ready_future<>();
    });
}


seastar::future<>
LogStreamBase::_activeAndPersistTheFirstPlog(){
    String plogId = _preallocatedPlogPool.back();
    _preallocatedPlogPool.pop_back();
    PlogInfo info{.currentOffset=0, .sealed=false, .next_plogId=""};
    _first_plogId = plogId;
    _current_plogId = plogId;
    _usedPlogInfo[std::move(plogId)] = std::move(info);
    
    // persist this used plog info
    return _addNewPlog(0, _current_plogId)
    .then([this] (auto&& response){
        if (!response.is2xxOK()) {
            throw k2::dto::LogStreamBasePersistError("unable to persist metadata");
        }
        _create = true;
        return seastar::make_ready_future<>();
    });
}
seastar::future<std::pair<String, uint32_t> > 
LogStreamBase::append(Payload payload, String plogId, uint32_t offset){
    if (!_create){
        throw k2::dto::LogStreamBaseExistError("LogStreamBase does not created");
    }
    if (_current_plogId != plogId || _usedPlogInfo[_current_plogId].sealed || _usedPlogInfo[_current_plogId].currentOffset != offset){
        throw k2::dto::LogStreamBaseExistError("LogStreamBase append request information inconsistent");
    }
    return append(std::move(payload));
};


seastar::future<std::pair<String, uint32_t> > 
LogStreamBase::append(Payload payload){
    if (!_create){
        throw k2::dto::LogStreamBaseExistError("LogStreamBase does not created");
    }
    String plogId = _current_plogId;

    if (_usedPlogInfo[plogId].currentOffset + payload.getSize() > PLOG_MAX_SIZE){
        // switch to a new plog
        return _switchPlogAndAppend(std::move(payload));
    }
    else{
        uint32_t current_offset = _usedPlogInfo[plogId].currentOffset;
        _usedPlogInfo[plogId].currentOffset += payload.getSize();
        uint32_t expect_appended_offset = _usedPlogInfo[plogId].currentOffset;
        return _client.append(plogId, current_offset, std::move(payload)).
        then([this, plogId, expect_appended_offset] (auto&& response){
            auto& [status, appended_offset] = response;

            if (!status.is2xxOK() || expect_appended_offset != appended_offset) {
                throw k2::dto::PlogAppendError("unable to append a plog");
            }
            // Check weather there is a sealed request that did not receive the response
            if (_switched){
                _switchRequestWaiters.emplace_back(seastar::promise<>());
                // if there is a flying sealed request, we should not notify the client until we receive the response of that sealed request
                return _switchRequestWaiters.back().get_future().
                then([plogId, appended_offset] (){
                    return seastar::make_ready_future<std::pair<String, uint32_t> >(std::make_pair(std::move(plogId), appended_offset));
                });
            }
            else{
                return seastar::make_ready_future<std::pair<String, uint32_t> >(std::make_pair(std::move(plogId), appended_offset));
            }
        });
    }
};

seastar::future<std::pair<String, uint32_t> > 
LogStreamBase::_switchPlogAndAppend(Payload payload){
    if (_preallocatedPlogPool.size() == 0){
        throw k2::dto::LogStreamBaseRedundantPlogError("no available redundant plog");
    }
    String sealed_plogId = _current_plogId;
    PlogInfo& targetPlogInfo = _usedPlogInfo[sealed_plogId];
    uint32_t sealed_offest = targetPlogInfo.currentOffset;
    
    String new_plogId = _preallocatedPlogPool.back();
    _preallocatedPlogPool.pop_back();
    PlogInfo info{.currentOffset=0, .sealed=false, .next_plogId=""};
    _usedPlogInfo[new_plogId] = std::move(info);
    _usedPlogInfo[sealed_plogId].next_plogId = new_plogId;
    
    _switched = true;
    _usedPlogInfo[new_plogId].currentOffset += payload.getSize();
    _current_plogId = new_plogId;

    uint32_t expect_appended_offset =  _usedPlogInfo[new_plogId].currentOffset;

    // The following process could be asynchronous 
    std::vector<seastar::future<>> waitFutures;
    // Append to the new Plog
    waitFutures.push_back(_client.append(new_plogId, 0, std::move(payload))
    .then([expect_appended_offset] (auto&& response){
        auto& [status, appended_offset] = response;
        if (!status.is2xxOK() || expect_appended_offset != appended_offset) {
            throw k2::dto::PlogAppendError("unable to append a plog");
        }
        return seastar::make_ready_future<>();
    }));
    // Seal the old Plog
    waitFutures.push_back(_client.seal(sealed_plogId, sealed_offest)
    .then([this, sealed_plogId] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::PlogAppendError("unable to seal a plog");
        }
        _usedPlogInfo[sealed_plogId].sealed = true;
        return seastar::make_ready_future<>();
    }));
    // Preallocated a new plog
    waitFutures.push_back(_client.create()
    .then([this] (auto&& response){
        auto& [status, plogId] = response;
        if (!status.is2xxOK()){
            throw k2::dto::PlogCreateError("unable to create plog for Logstream Base");
        }
        _preallocatedPlogPool.push_back(std::move(plogId));
        return seastar::make_ready_future<>();
    }));
    // Persist the metadata of sealed Plog's Offset and new PlogId
    waitFutures.push_back(_addNewPlog(sealed_offest, new_plogId)
    .then([this] (auto&& response){
        auto& status = response;
        if (!status.is2xxOK()){
            throw k2::dto::PlogCreateError("unable to create plog for Logstream Base");
        }
        return seastar::make_ready_future<>();
    }));

    // Clear the switchRequestWaiters
    return seastar::when_all_succeed(waitFutures.begin(), waitFutures.end())
    .then([this, new_plogId, expect_appended_offset] (){
        _switched = false;
        // send all the pending response to client 
        for (auto& request: _switchRequestWaiters){
            request.set_value();
        }
        _switchRequestWaiters.clear();
        return seastar::make_ready_future<std::pair<String, uint32_t> >(std::make_pair(std::move(new_plogId), expect_appended_offset));
    });
}


seastar::future<std::vector<Payload> > 
LogStreamBase::read(String start_plogId, uint32_t start_offset, uint32_t size){
    auto it = _usedPlogInfo.find(start_plogId);
    if (it == _usedPlogInfo.end()) {
        return seastar::make_exception_future<std::vector<Payload> >(std::runtime_error("unable to find start plogId"));
    }

    std::vector<MetadataElement> metadataInfo;
    String plogId = start_plogId;
    while (size != 0){
        if (plogId == ""){
            return seastar::make_exception_future<std::vector<Payload> >(std::runtime_error("request read size overflow"));
        }
        PlogInfo& targetPlogInfo = _usedPlogInfo[plogId];
        MetadataElement log_entry{.plogId=plogId, .start_offset=start_offset, .size=0};
        if (targetPlogInfo.currentOffset - start_offset >= size){
            log_entry.size = size;
            size = 0;

        }
        else{
            log_entry.size = targetPlogInfo.currentOffset;
            size -= targetPlogInfo.currentOffset - start_offset;
            start_offset = 0;
        }
        log_entry.size += log_entry.start_offset;
        metadataInfo.push_back(std::move(log_entry));
        plogId = _usedPlogInfo[plogId].next_plogId;
    }

    _stopped = false;
    uint32_t current_offset = metadataInfo[0].start_offset;
    uint32_t index = 0;
    std::vector<Payload> payload_list;

    return seastar::do_with(std::move(payload_list), std::move(index), std::move(current_offset), std::move(metadataInfo), [&] (auto& payload_list, auto& index, auto& current_offset, auto& metadataInfo){
        return seastar::do_until(
            [this] { return _stopped; },
            [&] {
                String plogId = metadataInfo[index].plogId;
                uint32_t start_offset = current_offset;
                uint32_t request_size;
                
                if (current_offset + PLOG_MAX_READ_SIZE < metadataInfo[index].size){
                    request_size = PLOG_MAX_READ_SIZE;
                    current_offset += request_size;
                }
                else{
                    request_size = metadataInfo[index].size - current_offset;
                    ++index;
                    if (index >= metadataInfo.size()){
                        _stopped = true;
                    }
                    else{
                        current_offset = metadataInfo[index].start_offset;
                    }
                }
                return _client.read(plogId, start_offset, request_size)
                .then([&] (auto&& response){
                    auto& [status, payload] = response;
                    if (!status.is2xxOK()){
                        throw k2::dto::PlogReadError("unable to read a plog");
                    }
                    payload_list.push_back(std::move(payload));
                    return seastar::make_ready_future<>();
                });
            }
        )
        .then([&] (){
            return seastar::make_ready_future<std::vector<Payload> >(std::move(payload_list));
        });
    });
    /*std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
    for (auto& log_entry: metadataInfo){
        readFutures.push_back(_client.read(log_entry.plogId, log_entry.start_offset, log_entry.size));
    }

    return seastar::when_all_succeed(readFutures.begin(), readFutures.end())
    .then([this] (std::vector<std::tuple<Status, Payload> >&& responses){
        std::vector<Payload> outputPayloads;
        for (auto& response: responses){
            auto& [status, resp] = response;
            if (!status.is2xxOK()){
                throw k2::dto::PlogReadError("unable to read a plog");
            }
            outputPayloads.push_back(std::move(resp));
        }
        return seastar::make_ready_future<std::vector<Payload> >(std::move(outputPayloads));
    });*/
}


seastar::future<Status> 
LogStreamBase::reload(std::vector<dto::MetadataRecord> plogsOfTheStream){
    K2LOG_I(log::lgbase, "LogStreamBase Reload");
    if (_create){
        throw k2::dto::LogStreamBaseExistError("Log stream already created");
    }
    _switched = false;
    
    String previous_plogId = "";
    for (auto& record: plogsOfTheStream){
        PlogInfo info{.currentOffset=record.sealed_offset, .sealed=true, .next_plogId=""};
        _usedPlogInfo[record.plogId] = std::move(info);
        if (previous_plogId == ""){
            _first_plogId = record.plogId;
        }
        else{
            _usedPlogInfo[previous_plogId].next_plogId = record.plogId;
        }
        _current_plogId = record.plogId;
        previous_plogId = std::move(record.plogId);
    }
    _usedPlogInfo[_current_plogId].sealed=false;

    return _preallocatePlogs()
    .then([this] (){
        _create = true;
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully reload metadata"));
    });
}

seastar::future<std::tuple<Status, std::tuple<uint32_t, bool>>> 
LogStreamBase::get_plog_status(String plogId){
    return _client.getPlogStatus(std::move(plogId));
}
LogStream::LogStream() {
    K2LOG_I(log::lgbase, "dtor");
}

LogStream::~LogStream() {
    K2LOG_I(log::lgbase, "dtor");
}

seastar::future<> 
LogStream::init(Verb name, MetadataMgr* metadataMgr, String cpo_url, String persistenceClusterName, bool reload){
    _name = name;
    _metadataMgr = metadataMgr;
    return _init_plog_client(cpo_url, persistenceClusterName)
    .then([this] (){
        return _preallocatePlogs();
    })
    .then([this, reload] (){
        if (!reload){
            return _activeAndPersistTheFirstPlog();
        }
        else{
            return seastar::make_ready_future<>();
        }
    });
}

seastar::future<Status>
LogStream::_addNewPlog(uint32_t sealed_offset, String new_plogId){
    return _metadataMgr->addNewPLogIntoLogStream(_name, sealed_offset, std::move(new_plogId));
}


MetadataMgr::MetadataMgr() {
    K2LOG_I(log::lgbase, "dtor");
}

MetadataMgr::~MetadataMgr() {
    K2LOG_I(log::lgbase, "dtor");
}

seastar::future<> 
MetadataMgr::init(String cpo_url, String partitionName, String persistenceClusterName, bool reload){
    _cpo = CPOClient(cpo_url);
    _partitionName = std::move(partitionName);
    return _init_plog_client(cpo_url, persistenceClusterName)
    .then([this] (){
        return _preallocatePlogs();
    })
    .then([this, reload] (){
        if (!reload){
            return _activeAndPersistTheFirstPlog();
        }
        else{
            return seastar::make_ready_future<>();
        }
    })
    .then([this, cpo_url, persistenceClusterName, reload] (){
        std::vector<seastar::future<> > initFutures;
        for (int logstreamName=LogStreamType::LogStreamTypeHead+1; logstreamName<LogStreamType::LogStreamTypeEnd; ++logstreamName){
            LogStream* _logstream = new LogStream();
            _logStreamMap[logstreamName] = _logstream;
            initFutures.push_back(_logStreamMap[logstreamName]->init(logstreamName, this, cpo_url, persistenceClusterName, reload));
        }
        return seastar::when_all_succeed(initFutures.begin(), initFutures.end());
    })
    .then([] (){
        return seastar::make_ready_future<>();
    });
}

seastar::future<Status>
MetadataMgr::_addNewPlog(uint32_t sealed_offset, String new_plogId){
    return _cpo.PersistMetadata(Deadline<>(_cpo_timeout()), _partitionName, sealed_offset, std::move(new_plogId)).
    then([this] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::MetadataPersistError("unable to persist metadata to CPO");
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
    return append(std::move(temp_payload))
    .then([this] (auto&& response){
        auto& [plogId, appended_offset] = response;
        K2LOG_D(log::lgbase, "{}, {}", plogId, appended_offset);
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully persist metadata"));
    });
}

seastar::future<Status> 
MetadataMgr::replay(String cpo_url, String partitionName, String persistenceClusterName){
    return init(cpo_url, partitionName, persistenceClusterName, true).
    then([&] (){
        return _cpo.GetMetadata(Deadline<>(_cpo_timeout()), _partitionName);
    })
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::MetadataGetError("unable to get metadata from CPO");
        }
        std::vector<dto::MetadataRecord> records = std::move(resp.records);
        return seastar::do_with(std::move(records), [&] (auto& records){
            return get_plog_status(records.back().plogId)
            .then([&] (auto&& response){
                auto& [status, resp] = response;
                if (!status.is2xxOK()) {
                    throw k2::dto::PlogGetStatusError("unable to obtain the information of a plog");
                }

                records.back().sealed_offset = std::get<0>(resp);
                return reload(records);
            })
            .then([&] (auto&& response){
                auto& status = response;
                if (!status.is2xxOK()) {
                    throw k2::dto::LogStreamBaseReload("unable to reload the metadata");
                }
                uint32_t read_size = 0;
                for (auto& record: records){
                    read_size += record.sealed_offset;
                }
                return read(records[0].plogId, 0, read_size);
            })
            .then([&] (auto&& payloads){
                std::unordered_map<Verb, std::vector<dto::MetadataRecord> > logStreamRecords;
                Verb logStreamName;
                uint32_t offset;
                String plogId;
                for (auto& payload:payloads){
                    payload.seek(0);
                    while (payload.getDataRemaining() > 0){
                        payload.read(logStreamName);
                        payload.read(offset);
                        payload.read(plogId);
                        auto it = logStreamRecords.find(logStreamName);
                        if (it == logStreamRecords.end()) {
                            std::vector<dto::MetadataRecord> log;
                            dto::MetadataRecord element{.plogId=plogId, .sealed_offset=0};
                            log.push_back(std::move(element));
                            logStreamRecords[logStreamName] = std::move(log);
                        }
                        else{
                            it->second.back().sealed_offset = offset;
                            dto::MetadataRecord element{.plogId=plogId, .sealed_offset=0};
                            it->second.push_back(std::move(element));
                        }
                    }
                }

                return seastar::do_with(std::move(logStreamRecords), [&] (auto& logStreamRecords){
                    std::vector<seastar::future<std::tuple<Status, std::tuple<uint32_t, bool> > > > getStatusFutures;
                    for (int logstreamName=LogStreamType::LogStreamTypeHead+1; logstreamName<LogStreamType::LogStreamTypeEnd; ++logstreamName){
                        getStatusFutures.push_back(_logStreamMap[logstreamName]->get_plog_status(logStreamRecords[logstreamName].back().plogId));
                    }
                    return seastar::when_all_succeed(getStatusFutures.begin(), getStatusFutures.end())
                    .then([&] (auto&& responses){
                        Verb logstreamName=LogStreamType::LogStreamTypeHead;
                        std::vector<seastar::future<Status> > reloadFutures;
                        for (auto& response: responses){
                            auto& [status, resp] = response;
                            if (!status.is2xxOK()) {
                                throw k2::dto::PlogGetStatusError("unable to obtain the information of a plog");
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
                                throw k2::dto::LogStreamBaseReload("unable to reload the metadata");
                            }
                        }
                        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully persist metadata"));
                    });
                });
            });
        });
    });
}

} // k2