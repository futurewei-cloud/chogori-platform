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
        throw std::runtime_error("Log stream already created");
    }

    _logStreamName = std::move(logStreamName);
    _logStreamCreate = true;

    // create metadata plogs in advance
    std::vector<seastar::future<std::tuple<Status, String> > > createFutures;
    for (uint32_t i = 0; i < METADATA_PLOG_POOL_SIZE; ++i){
        createFutures.push_back(_client.create());
    }
    return seastar::when_all_succeed(createFutures.begin(), createFutures.end())
    .then([this] (auto&& responses){
        // register the metadata plog info to CPO
        for (auto& response: responses){
            auto& [status, plogId] = response;
            if (!status.is2xxOK()){
                throw std::runtime_error("unable to create plog for Metadata LogStream");
            }
            _metadataPlogPool.push_back(std::move(plogId));
        }
        String plogId = _metadataPlogPool[_metadataPlogPool.size()-1];
        _metadataPlogPool.pop_back();
        _plogInfo.push_back(std::make_pair(std::move(plogId), 0));
        return _cpo.RegisterMetadataStreamLog(Deadline<>(_cpo_timeout()), _logStreamName, _plogInfo[0].first);
    })
    .then([this] (auto&& response){
        // create WAL plogs in advance
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to register Metadata LogStream Plog to CPO");
        }
        std::vector<seastar::future<std::tuple<Status, String> > > createFutures;
        for (uint32_t i = 0; i < WAL_PLOG_POOL_SIZE; ++i){
            createFutures.push_back(_client.create());
        }
        return seastar::when_all_succeed(createFutures.begin(), createFutures.end());
    })
    .then([this] (auto&& responses){
        for (auto& response: responses){
            auto& [status, plogId] = response;
            if (!status.is2xxOK()){
                throw std::runtime_error("unable to create plog for WAL");
            }
            _walPlogPool.push_back(std::move(plogId));
        }
        String plogId = _walPlogPool[_walPlogPool.size()-1];
        _walPlogPool.pop_back();
        _plogInfo.push_back(std::make_pair(plogId, 0));

        // write the metadata of the first WAL plog to metadata log streams
        Payload temp_payload([] { return Binary(4096); });
        temp_payload.write(std::move(plogId));
        return _write(std::move(temp_payload), 0);
    });
}

seastar::future<std::vector<Payload> >
LogStream::read(String logStreamName){
    K2INFO("Received Logstream Read Request for " << logStreamName);
    // read the metadata of metadata log stream from CPO
    return _cpo.GetMetadataStreamLog(Deadline<>(_cpo_timeout()), logStreamName)
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to obtain metadata log stream");
        }
        _streamLog = std::move(resp.streamLog);
        return _client.info(_streamLog[_streamLog.size()-1].name);
    })
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to obtain the information of a plog");
        }

        // read the metadata information of WAL plogs from metadata log stream
        _streamLog[_streamLog.size()-1].offset = std::get<0>(resp);
        std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
        for (auto& log_entry: _streamLog){
            readFutures.push_back(_client.read(log_entry.name, 0, log_entry.offset));
        }
        return seastar::when_all_succeed(readFutures.begin(), readFutures.end());
    })
    .then([this] (std::vector<std::tuple<Status, Payload> >&& responses){
        std::vector<Payload> payloads;
        for (auto& response: responses){
            auto& [status, resp] = response;
            if (!status.is2xxOK()){
                throw std::runtime_error("unable to read a plog");
            }
            payloads.push_back(std::move(resp));
        }
        // read the contents from WAL 
        return _readContent(std::move(payloads));
    });
}

seastar::future<std::vector<Payload> >
LogStream::_readContent(std::vector<Payload> payloads){
    //Format: plogId1, offset1, plogId2, offset2... 
    _streamLog.clear();
    for (auto& payload:payloads){
        payload.seek(0);
        while (payload.getDataRemaining() > 0){
            String plogId;
            uint32_t offset;
            payload.read(plogId);
            if (payload.getDataRemaining() > 0){
                payload.read(offset);
            }
            else{
                offset = 0;
            }
            dto::MetadataElement log_entry{.name=std::move(plogId), .offset=offset};
            _streamLog.push_back(std::move(log_entry));
        } 
        payload.clear();
    }
    return _client.info(_streamLog[_streamLog.size()-1].name)
    .then([this] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to obtain the information of a plog");
        }
        _streamLog[_streamLog.size()-1].offset = std::get<0>(resp);
        std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
        for (auto& log_entry: _streamLog){
            readFutures.push_back(_client.read(log_entry.name, 0, log_entry.offset));
        }
        return seastar::when_all_succeed(readFutures.begin(), readFutures.end());
    })
    .then([this] (std::vector<std::tuple<Status, Payload> >&& responses){
        std::vector<Payload> newPayloads;
        for (auto& response: responses){
            auto& [status, resp] = response;
            if (!status.is2xxOK()){
                throw std::runtime_error("unable to read a plog");
            }
            newPayloads.push_back(std::move(resp));
        }
        return seastar::make_ready_future<std::vector<Payload> >(std::move(newPayloads));
    });
}


seastar::future<> 
LogStream::write(Payload payload){
    return _write(std::move(payload), 1);
}

seastar::future<> 
LogStream::_write(Payload payload, uint32_t writeToWAL){
    if (!_logStreamCreate){
        K2INFO("Log stream does not create");
        throw std::runtime_error("Log stream does not create");
    }

    String sealed_plogId;
    String new_plogId;
    uint32_t sealed_offest = 0;
    
    // determine whether the current plog will excceed the size limit after the incoming writing operation
    if (_plogInfo[writeToWAL].second + payload.getSize() >= PLOG_MAX_SIZE){
        sealed_plogId = _plogInfo[writeToWAL].first;
        sealed_offest = _plogInfo[writeToWAL].second;
        // change to the next available plog id
        if (writeToWAL == 0){
            new_plogId = _metadataPlogPool[_metadataPlogPool.size()-1];
            _metadataPlogPool.pop_back();
        }
        else{
            new_plogId = _walPlogPool[_walPlogPool.size()-1];
            _walPlogPool.pop_back();
        }
        _plogInfo[writeToWAL].first = new_plogId;
        _plogInfo[writeToWAL].second = 0;
    }

    uint32_t current_offset = _plogInfo[writeToWAL].second;
    _plogInfo[writeToWAL].second += payload.getSize();

    // write the incoming payload to current plog
    return _client.append(_plogInfo[writeToWAL].first, current_offset,  std::move(payload))
    .then([&, writeToWAL, sealed_plogId, new_plogId, sealed_offest] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            K2INFO("unable to append the plog");
            K2INFO(status);
            throw std::runtime_error("unable to append the plog");
        }
        // If sealed_offest == 0, then it means after the incoming writing operation, the current plog is not excceed the size limit
        if (sealed_offest == 0){
            return seastar::make_ready_future<>();
        }
        // It the previous plog excceed the size limit, we will seal the previous plog and write the metadata to corrsponding place (Metadata Log Stream or CPO)
        return _client.seal(sealed_plogId, sealed_offest)
        .then([&, writeToWAL, new_plogId, sealed_offest] (auto&& response){
            auto& [status, resp] = response;
            if (!status.is2xxOK()) {
                throw std::runtime_error("unable to seal a plog");
            }

            // Write the metadata to metadata log stream
            if (writeToWAL){
                Payload temp_payload([] { return Binary(4096); });
                temp_payload.write(sealed_offest);
                temp_payload.write(std::move(new_plogId));
                return _write(std::move(temp_payload), 0);
            }
            // Write the metadata to CPO
            else{
                return _cpo.UpdateMetadataStreamLog(Deadline<>(_cpo_timeout()), _logStreamName, sealed_offest, std::move(new_plogId))
                .then([] (auto&& response){
                    auto& [status, resp] = response;
                    if (!status.is2xxOK()) {
                        throw std::runtime_error("unable to update MSL to CPO");
                    }
                    return seastar::make_ready_future<>();
                });
            }
        });
    });
}

} // k2
