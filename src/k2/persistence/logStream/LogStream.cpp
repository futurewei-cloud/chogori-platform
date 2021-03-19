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
LogStreamBase::init_plog_client(String cpo_url, String persistenceClusrerName){
    return _client.init(persistenceClusrerName, cpo_url);
}

seastar::future<> 
LogStreamBase::create(){
    K2LOG_I(log::lgbase, "LogStreamBase Create");
    if (_create){
        throw k2::dto::LogStreamBaseExistError("Log stream already created");
    }
    _switched = false;

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
            _redundantPlogPool.push_back(std::move(plogId));
        }
        String plogId = _redundantPlogPool[_redundantPlogPool.size()-1];
        _redundantPlogPool.pop_back();
        _usedPlogIdVector.push_back(plogId);
        PlogInfo info{.currentOffset=0, .sealed=false, .index=(uint32_t)_usedPlogIdVector.size()-1};
        _usedPlogInfo[std::move(plogId)] = std::move(info);
        
        // persist this used plog info
        return _persistSelfMetadata(0, _usedPlogIdVector[_usedPlogIdVector.size()-1]);
    })
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
    if (_usedPlogIdVector[_usedPlogIdVector.size()-1] != plogId || _usedPlogInfo[_usedPlogIdVector[_usedPlogIdVector.size()-1]].sealed || _usedPlogInfo[_usedPlogIdVector[_usedPlogIdVector.size()-1]].currentOffset != offset){
        throw k2::dto::LogStreamBaseExistError("LogStreamBase append request information inconsistent");
    }
    return append(std::move(payload));
};


seastar::future<std::pair<String, uint32_t> > 
LogStreamBase::append(Payload payload){
    if (!_create){
        throw k2::dto::LogStreamBaseExistError("LogStreamBase does not created");
    }
    String plogId = _usedPlogIdVector[_usedPlogIdVector.size()-1];

    if (_usedPlogInfo[plogId].currentOffset + payload.getSize() < PLOG_MAX_SIZE){
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
                _requestWaiters.emplace_back(seastar::promise<>());
                // if there is a flying sealed request, we should not notify the client until we receive the response of that sealed request
                return _requestWaiters.back().get_future().
                then([plogId, appended_offset] (){
                    return seastar::make_ready_future<std::pair<String, uint32_t> >(std::make_pair(std::move(plogId), appended_offset));
                });
            }
            else{
                return seastar::make_ready_future<std::pair<String, uint32_t> >(std::make_pair(std::move(plogId), appended_offset));
            }
        });
    }
    else{
        // switch to a new plog
        return _switchPlog(std::move(payload));
    }
};

seastar::future<std::pair<String, uint32_t> > 
LogStreamBase::_switchPlog(Payload payload){
    if (_redundantPlogPool.size() == 0){
        throw k2::dto::LogStreamBaseRedundantPlogError("no available redundant plog");
    }
    String sealed_plogId = _usedPlogIdVector[_usedPlogIdVector.size()-1];
    PlogInfo& targetPlogInfo = _usedPlogInfo[sealed_plogId];
    uint32_t sealed_offest = targetPlogInfo.currentOffset;
    
    String new_plogId = _redundantPlogPool[_redundantPlogPool.size()-1];
    _redundantPlogPool.pop_back();
    _usedPlogIdVector.push_back(new_plogId);
    PlogInfo info{.currentOffset=0, .sealed=false, .index=(uint32_t)_usedPlogIdVector.size()-1};
    _usedPlogInfo[new_plogId] = std::move(info);
    
    _switched = true;
    _usedPlogInfo[new_plogId].currentOffset += payload.getSize();

    uint32_t expect_appended_offset =  _usedPlogInfo[new_plogId].currentOffset;

    return _client.append(new_plogId, 0, std::move(payload)).
    then([this, new_plogId, sealed_plogId, sealed_offest, expect_appended_offset] (auto&& response){
        auto& [status, appended_offset] = response;
        if (!status.is2xxOK() || expect_appended_offset != appended_offset) {
            throw k2::dto::PlogAppendError("unable to append a plog");
        }
        return _client.seal(sealed_plogId, sealed_offest);
    }).
    then([this, new_plogId, sealed_plogId, sealed_offest, expect_appended_offset] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::PlogAppendError("unable to seal a plog");
        }
        _usedPlogInfo[sealed_plogId].sealed = true;
        // create a redundant plog
        return _client.create();
    }).
    then([this, new_plogId, sealed_offest, expect_appended_offset] (auto&& response){
        auto& [status, plogId] = response;
        if (!status.is2xxOK()){
            throw k2::dto::PlogCreateError("unable to create plog for Logstream Base");
        }
        _redundantPlogPool.push_back(std::move(plogId));
        return _persistSelfMetadata(sealed_offest, new_plogId);
    }).
    then([this, new_plogId, expect_appended_offset] (auto&& response){
        if (!response.is2xxOK()) {
            throw k2::dto::LogStreamBasePersistError("unable to persist metadata");
        }
        _switched = false;
        // send all the pending response to client 
        for (auto& request: _requestWaiters){
            request.set_value();
        }
        _requestWaiters.clear();
        return seastar::make_ready_future<std::pair<String, uint32_t> >(std::make_pair(std::move(new_plogId), expect_appended_offset));
    });
    
}

seastar::future<std::vector<Payload> > 
LogStreamBase::read(String start_plogId, uint32_t start_offset, uint32_t size){
    auto it = _usedPlogInfo.find(start_plogId);
    if (it == _usedPlogInfo.end()) {
        throw k2::dto::LogStreamBaseReadError("unable to find start plogId");
    }

    std::vector<MetadataElement> metadataInfo;
    uint32_t index = it->second.index;
    while (size != 0){
        if (index >= _usedPlogIdVector.size()){
            throw k2::dto::LogStreamBaseReadError("request read size overflow");
        }
        PlogInfo& targetPlogInfo = _usedPlogInfo[_usedPlogIdVector[index]];
        MetadataElement log_entry{.plogId=_usedPlogIdVector[index], .start_offset=start_offset, .size=0};
        if (targetPlogInfo.currentOffset - start_offset >= size){
            log_entry.size = size;
            size = 0;
        }
        else{
            log_entry.size = targetPlogInfo.currentOffset;
            size -= targetPlogInfo.currentOffset - start_offset;
            start_offset = 0;
        }
        metadataInfo.push_back(std::move(log_entry));
        ++index;
    }

    std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
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
    });
}


seastar::future<Status> 
LogStreamBase::reload(std::vector<dto::MetadataRecord> records){
    K2LOG_I(log::lgbase, "LogStreamBase Reload");
    if (_create){
        throw k2::dto::LogStreamBaseExistError("Log stream already created");
    }
    _switched = false;
    for (auto& record: records){
        _usedPlogIdVector.push_back(record.plogId);
        PlogInfo info{.currentOffset=record.sealed_offset, .sealed=true, .index=(uint32_t)_usedPlogIdVector.size()-1};
        _usedPlogInfo[std::move(record.plogId)] = std::move(info);
    }
    _usedPlogInfo[_usedPlogIdVector[_usedPlogIdVector.size()-1]].sealed=false;

    std::vector<seastar::future<std::tuple<Status, String> > > createFutures;
    for (uint32_t i = 0; i < PLOG_POOL_SIZE; ++i){
        createFutures.push_back(_client.create());
    }
    return seastar::when_all_succeed(createFutures.begin(), createFutures.end())
    .then([this] (auto&& responses){
        // persist the plog info 
        for (auto& response: responses){
            auto& [status, plogId] = response;
            if (!status.is2xxOK()){
                throw k2::dto::PlogCreateError("unable to create plog for Logstream Base");
            }
            _redundantPlogPool.push_back(std::move(plogId));
        }
        _create = true;
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully reload metadata"));
    });
}

seastar::future<std::tuple<Status, std::tuple<uint32_t, bool>>> 
LogStreamBase::get_plog_status(String plogId){
    return _client.getPlogStatus(std::move(plogId));
}
LogStream::LogStream() {
    K2LOG_I(log::logstream, "dtor");
}

LogStream::~LogStream() {
    K2LOG_I(log::logstream, "dtor");
}

seastar::future<> 
LogStream::init(String name, MetadataMgr* metadataMgr, String cpo_url, String persistenceClusrerName){
    _name = name;
    _metadataMgr = metadataMgr;
    return init_plog_client(cpo_url, persistenceClusrerName);
}

seastar::future<Status>
LogStream::_persistSelfMetadata(uint32_t sealed_offset, String new_plogId){
    return _metadataMgr->persistMetadata(_name, sealed_offset, std::move(new_plogId));
}


MetadataMgr::MetadataMgr() {
    K2LOG_I(log::mdmgr, "dtor");
}

MetadataMgr::~MetadataMgr() {
    K2LOG_I(log::mdmgr, "dtor");
}

seastar::future<> 
MetadataMgr::init(String cpo_url, String partitionName, String persistenceClusrerName){
    _cpo = CPOClient(cpo_url);
    _partitionName = std::move(partitionName);
    return _wal.init("WAL", this, cpo_url, persistenceClusrerName)
    .then([this, cpo_url, persistenceClusrerName] (){
        _logStreamMap["WAL"] = &_wal;
        return init_plog_client(cpo_url, persistenceClusrerName);
    });
}

seastar::future<Status>
MetadataMgr::_persistSelfMetadata(uint32_t sealed_offset, String new_plogId){
    return _cpo.PersistMetadata(Deadline<>(_cpo_timeout()), _partitionName, sealed_offset, std::move(new_plogId)).
    then([this] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::MetadataPersistError("unable to persist metadata to CPO");
        }
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully persist metadata"));
    });
}

LogStream* MetadataMgr::obtainLogStream(String log_stream_name){
    auto it = _logStreamMap.find(log_stream_name);
    if (it == _logStreamMap.end()) {
        throw k2::dto::LogStreamRetrieveError("unable to retrieve the target logstream");
    }
    return it->second;
}


seastar::future<Status> 
MetadataMgr::persistMetadata(String name, uint32_t sealed_offset, String new_plogId){
    Payload temp_payload(Payload::DefaultAllocator);
    temp_payload.write(name);
    temp_payload.write(sealed_offset);
    temp_payload.write(std::move(new_plogId));
    return append(std::move(temp_payload))
    .then([this] (auto&& response){
        auto& [plogId, appended_offset] = response;
        K2LOG_D(log::mdmgr, "{}, {}", plogId, appended_offset);
        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully persist metadata"));
    });
}

seastar::future<Status> 
MetadataMgr::replay(String cpo_url, String partitionName, String persistenceClusrerName){
    return init(cpo_url, partitionName, persistenceClusrerName).
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
            return get_plog_status(records[records.size()-1].plogId)
            .then([&] (auto&& response){
                auto& [status, resp] = response;
                if (!status.is2xxOK()) {
                    throw k2::dto::PlogGetStatusError("unable to obtain the information of a plog");
                }

                records[records.size()-1].sealed_offset = std::get<0>(resp);
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
                std::unordered_map<String, std::vector<dto::MetadataRecord> > logStreamRecords;
                String logStreamName;
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
                            it->second[it->second.size()-1].sealed_offset = offset;
                            dto::MetadataRecord element{.plogId=plogId, .sealed_offset=0};
                            it->second.push_back(std::move(element));
                        }
                    }
                }

                return seastar::do_with(std::move(logStreamRecords), [&] (auto& logStreamRecords){
                    return _wal.get_plog_status(logStreamRecords["WAL"][logStreamRecords["WAL"].size()-1].plogId)
                    .then([&] (auto&& response){
                        auto& [status, resp] = response;
                        if (!status.is2xxOK()) {
                            throw k2::dto::PlogGetStatusError("unable to obtain the information of a plog");
                        }
                        logStreamRecords["WAL"][logStreamRecords["WAL"].size()-1].sealed_offset = std::get<0>(resp);
                        return _wal.reload(std::move(logStreamRecords["WAL"]));
                    })
                    .then([this] (auto&& response){
                        auto& status = response;
                        if (!status.is2xxOK()) {
                            throw k2::dto::LogStreamBaseReload("unable to reload the metadata");
                        }
                        return seastar::make_ready_future<Status>(Statuses::S200_OK("successfully persist metadata"));
                    });
                });
            });
        });
    });
}

} // k2