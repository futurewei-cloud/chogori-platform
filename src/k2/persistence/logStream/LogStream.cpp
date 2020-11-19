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
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RPCTypes.h>
#include <k2/transport/Status.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/LogStream.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>
#include <random>

namespace k2 {

LogStream::LogStream() {
    K2INFO("dtor");    
}

LogStream::~LogStream() {
    K2INFO("~dtor");
}

seastar::future<> 
LogStream::init(String cpo_url, String persistenceClusrerName){
    K2INFO("LogStream Init");
    _cpo = CPOClient(cpo_url);
    return _client.init(persistenceClusrerName, cpo_url);
}

seastar::future<>
LogStream::create(String logStreamName){
    if (_logStreamCreate){
        K2INFO("Log streaam already created");
        throw k2::dto::LogStreamExistError("Log stream already created");
    }

    _logStreamName = std::move(logStreamName);
    _logStreamCreate = true;

    // create metadata plogs in advance
    std::vector<seastar::future<std::tuple<Status, String> > > createFutures;
    for (uint32_t i = 0; i < METADATA_PLOG_POOL_SIZE + 1; ++i){
        createFutures.push_back(_client.create());
    }
    return seastar::when_all_succeed(createFutures.begin(), createFutures.end())
    .then([this] (auto&& responses){
        // register the metadata plog info to CPO
        for (auto& response: responses){
            auto& [status, plogId] = response;
            if (!status.is2xxOK()){
                throw k2::dto::PlogCreateError("unable to create plog for Metadata LogStream");
            }
            _metadataPlogPool.push_back(std::move(plogId));
        }
        String plogId = _metadataPlogPool[_metadataPlogPool.size()-1];
        _metadataPlogPool.pop_back();
        _metaInfo = std::move(std::make_pair(std::move(plogId), 0));
        return _cpo.RegisterMetadataStreamLog(Deadline<>(_cpo_timeout()), _logStreamName, _metaInfo.first);
    })
    .then([this] (auto&& response){
        // create WAL plogs in advance
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::LogStreamMetadataRegisterError("unable to register Metadata LogStream Plog to CPO");
        }
        std::vector<seastar::future<std::tuple<Status, String> > > createFutures;
        for (uint32_t i = 0; i < WAL_PLOG_POOL_SIZE + 1; ++i){
            createFutures.push_back(_client.create());
        }
        return seastar::when_all_succeed(createFutures.begin(), createFutures.end());
    })
    .then([this] (auto&& responses){
        for (auto& response: responses){
            auto& [status, plogId] = response;
            if (!status.is2xxOK()){
                throw k2::dto::PlogCreateError("unable to create plog for WAL");
            }
            _walPlogPool.push_back(std::move(plogId));
        }
        String plogId = _walPlogPool[_walPlogPool.size()-1];
        _walPlogPool.pop_back();
        _walInfo = std::move(std::make_pair(plogId, 0));

        // write the metadata of the first WAL plog to metadata log streams
        Payload temp_payload([] { return Binary(4096); });
        temp_payload.write(std::move(plogId));
        return _write(std::move(temp_payload), 0);
    });
}

seastar::future<std::vector<Payload> >
LogStream::read(String logStreamName){
    // read the metadata of metadata log stream from CPO
    return _cpo.GetMetadataStreamLog(Deadline<>(_cpo_timeout()), logStreamName)
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::LogStreamMetadataObtainError("unable to obtain metadata log stream");
        }
        std::vector<dto::MetadataElement> streamLog = std::move(resp.streamLog);
        return seastar::do_with(std::move(streamLog), [this] (auto& streamLog){
            return _client.info(streamLog[streamLog.size()-1].name)
            .then([&] (auto&& response){
                auto& [status, resp] = response;
                if (!status.is2xxOK()) {
                    throw k2::dto::PlogInfoError("unable to obtain the information of a plog");
                }

                // read the metadata information of WAL plogs from metadata log stream
                streamLog[streamLog.size()-1].offset = std::get<0>(resp);
                std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
                for (auto& log_entry: streamLog){
                    readFutures.push_back(_client.read(log_entry.name, 0, log_entry.offset));
                }
                return seastar::when_all_succeed(readFutures.begin(), readFutures.end());
            })
            .then([this] (std::vector<std::tuple<Status, Payload> >&& responses){
                std::vector<Payload> payloads;
                for (auto& response: responses){
                    auto& [status, resp] = response;
                    if (!status.is2xxOK()){
                        throw k2::dto::PlogReadError("unable to read a plog");
                    }
                    payloads.push_back(std::move(resp));
                }
                // read the contents from WAL 
                return _readContent(std::move(payloads));
            });
        });
        
    });
}

seastar::future<std::vector<Payload> >
LogStream::_readContent(std::vector<Payload> payloads){
    std::vector<dto::MetadataElement> streamLog;

    //Format: plogId1, offset1, plogId2, offset2... 
    bool readId = true;
    String pending_plogId;
    for (auto& payload:payloads){
        payload.seek(0);
        while (payload.getDataRemaining() > 0){ 
            if (readId){
                payload.read(pending_plogId);
                readId = false;
            }
            else{
                readId = true;
                uint32_t offset;
                payload.read(offset);
                dto::MetadataElement log_entry{.name=pending_plogId, .offset=offset};
                streamLog.push_back(std::move(log_entry));
            }
        } 
        payload.clear();
    }
    dto::MetadataElement log_entry{.name=pending_plogId, .offset=0};
    streamLog.push_back(std::move(log_entry));

    return seastar::do_with(std::move(streamLog), [this] (auto& streamLog){
        return _client.info(streamLog[streamLog.size()-1].name)
        .then([&] (auto&& response){
            auto& [status, resp] = response;
            if (!status.is2xxOK()) {
                throw k2::dto::PlogInfoError("unable to obtain the information of a plog");
            }
            streamLog[streamLog.size()-1].offset = std::get<0>(resp);
            std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
            for (auto& log_entry: streamLog){
                readFutures.push_back(_client.read(log_entry.name, 0, log_entry.offset));
            }
            return seastar::when_all_succeed(readFutures.begin(), readFutures.end());
        })
        .then([this] (std::vector<std::tuple<Status, Payload> >&& responses){
            std::vector<Payload> newPayloads;
            for (auto& response: responses){
                auto& [status, resp] = response;
                if (!status.is2xxOK()){
                    throw k2::dto::PlogReadError("unable to read a plog");
                }
                newPayloads.push_back(std::move(resp));
            }
            return seastar::make_ready_future<std::vector<Payload> >(std::move(newPayloads));
        });
    });
}


seastar::future<> 
LogStream::write(Payload payload){
    return _write(std::move(payload), true);
}

seastar::future<> 
LogStream::_write(Payload payload, bool writeToWAL){
    if (!_logStreamCreate){
        K2INFO("Log stream does not create");
        throw k2::dto::LogStreamExistError("Log stream does not create");
    }

    String sealed_plogId;
    String new_plogId;
    uint32_t sealed_offest = 0;
    
    std::pair<String, uint32_t>& targetPlogInfo = writeToWAL ? _walInfo : _metaInfo;
    // determine whether the current plog will excceed the size limit after the incoming writing operation
    if (targetPlogInfo.second + payload.getSize() >= PLOG_MAX_SIZE){
        sealed_plogId = targetPlogInfo.first;
        sealed_offest = targetPlogInfo.second;
        // change to the next available plog id
        if (!writeToWAL){
            if (_metadataPlogPool.size() == 0){
                throw k2::dto::LogStreamBackupPlogError("no available backup metadata plog");
            }
            new_plogId = _metadataPlogPool[_metadataPlogPool.size()-1];
            _metadataPlogPool.pop_back();
        }
        else{
            if (_walPlogPool.size() == 0){
                throw k2::dto::LogStreamBackupPlogError("no available backup wal plog");
            }
            new_plogId = _walPlogPool[_walPlogPool.size()-1];
            _walPlogPool.pop_back();
        }
        targetPlogInfo.first = new_plogId;
        targetPlogInfo.second = 0;
    }

    uint32_t current_offset = targetPlogInfo.second;
    targetPlogInfo.second += payload.getSize();

    return _client.append(targetPlogInfo.first, current_offset, std::move(payload))
    .then([&, writeToWAL, sealed_plogId, new_plogId, sealed_offest] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw k2::dto::PlogAppendError("unable to append the plog");
        }
        // If sealed_offest == 0, then it means after the incoming writing operation, the current plog is not excceed the size limit
        if (sealed_offest == 0){
            return seastar::make_ready_future<>();
        }
        
        return _client.seal(sealed_plogId, sealed_offest)
        .then([&, writeToWAL, new_plogId, sealed_offest] (auto&& response){
            auto& [status, resp] = response;
            if (!status.is2xxOK()) {
                throw k2::dto::PlogSealError("unable to seal a plog");
            }
            return _client.create();
        })
        .then([&, writeToWAL, new_plogId, sealed_offest] (auto&& response){
            auto& [status, plogId] = response;
            if (!status.is2xxOK()){
                throw k2::dto::PlogCreateError("unable to create a plog");
            }
                
            // Write the metadata to metadata log stream
            if (writeToWAL){
                _walPlogPool.push_back(std::move(plogId));
                Payload temp_payload([] { return Binary(4096); });
                temp_payload.write(sealed_offest);
                temp_payload.write(std::move(new_plogId));
                return _write(std::move(temp_payload), false);
            }
            // Write the metadata to CPO
            else{
                _metadataPlogPool.push_back(std::move(plogId));
                return _cpo.UpdateMetadataStreamLog(Deadline<>(_cpo_timeout()), _logStreamName, sealed_offest, std::move(new_plogId))
                .then([] (auto&& response){
                    auto& [status, resp] = response;
                    if (!status.is2xxOK()) {
                        throw k2::dto::LogStreamMetadataUpdateError("unable to update MSL to CPO");
                    }
                    return seastar::make_ready_future<>();
                });
            }
            
        });
    });
}

} // k2
