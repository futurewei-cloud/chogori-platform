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
LogStream::create(){
    if (_logStreamCreate){
        K2INFO("Log streaam already created");
        throw std::runtime_error("Log stream already created");
    }

    _logStreamName = _generateStreamName();
    _logStreamCreate = true;

    // create metadata stream plog
    return _client.create()
    .then([this] (auto&& response){
        // register the metadata plog info to CPO
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to create plog for MSL");
        }
        _plogInfo.push_back(std::make_pair(std::move(resp), 0));
        return _cpo.RegisterMetadataStreamLog(Deadline<>(_cpo_timeout()), _logStreamName, _plogInfo[0].first);
    })
    .then([this] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to register MSL to CPO");
        }
        return _client.create();
    })
    .then([this] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to create plog for WAL");
        }
        _plogInfo.push_back(std::make_pair(std::move(resp), 0));
        return seastar::make_ready_future<>();
    });
}

seastar::future<std::vector<Payload> >
LogStream::read(String logStreamName){
    K2INFO(logStreamName);
    std::vector<dto::MetadataElement> streamLog;
    return _cpo.GetMetadataStreamLog(Deadline<>(_cpo_timeout()), logStreamName)
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to obtain metadata log stream");
        }
        streamLog = std::move(resp.streamLog);
        return _client.info(streamLog[-1].name);
    })
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to obtain the information of a plog");
        }

        streamLog[-1].offset = std::get<0>(resp);
        std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
        for (auto& log_entry: streamLog){
            readFutures.push_back(_client.read(log_entry.name, 0, log_entry.offset));
        }
        return seastar::when_all_succeed(readFutures.begin(), readFutures.end());
    })
    .then([&] (std::vector<std::tuple<Status, Payload> >&& responses){
        std::vector<Payload> payloads;
        for (auto& response: responses){
            auto& [status, resp] = response;
            if (!status.is2xxOK()){
                throw std::runtime_error("unable to read a plog");
            }
            payloads.push_back(std::move(resp));
        }
        return _readContent(std::move(payloads));
    });
}

seastar::future<std::vector<Payload> >
LogStream::_readContent(std::vector<Payload> payloads){
    std::vector<dto::MetadataElement> streamLog;
    //Format: plogId1, offset1, plogId2, offset2... 
    for (auto& payload:payloads){
        payload.seek(0);
        while (payload.getDataRemaining() > 0){
            String plogId;
            uint32_t offset;
            payload.read(plogId);
            payload.read(offset);
            dto::MetadataElement log_entry{.name=std::move(plogId), .offset=offset};
            streamLog.push_back(std::move(log_entry));
        } 
        payload.clear();
    }
    return _client.info(streamLog[-1].name)
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        if (!status.is2xxOK()) {
            throw std::runtime_error("unable to obtain the information of a plog");
        }

        streamLog[-1].offset = std::get<0>(resp);
        std::vector<seastar::future<std::tuple<Status, Payload> > > readFutures;
        for (auto& log_entry: streamLog){
            readFutures.push_back(_client.read(log_entry.name, 0, log_entry.offset));
        }
        return seastar::when_all_succeed(readFutures.begin(), readFutures.end());
    })
    .then([&] (std::vector<std::tuple<Status, Payload> >&& responses){
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
    K2INFO("Received Write Operation");
    return _write(std::move(payload), true);
}

seastar::future<> 
LogStream::_write(Payload payload, bool writeToWAL){
    if (!_logStreamCreate){
        K2INFO("Log stream does not  create");
        throw std::runtime_error("Log stream does not create");
    }
    K2INFO("Write to WAL");
    Payload backup_payload = payload.shareAll();
    K2INFO("Start to Write");
    return _client.append(_plogInfo[writeToWAL].first, _plogInfo[writeToWAL].second, std::move(payload))
    .then([&] (auto&& response){
        auto& [status, resp] = response;
        K2INFO("Status " << status);
        if (status.is2xxOK()) {
            _plogInfo[writeToWAL].second = resp;
            K2INFO("Write Done");
            return seastar::make_ready_future<>();
        }
        else{
            K2INFO("Status " << status);
            return _client.seal(_plogInfo[writeToWAL].first, _plogInfo[writeToWAL].second)
            .then([&] (auto&& response){
                auto& [status, resp] = response;
                if (!status.is2xxOK()) {
                    throw std::runtime_error("unable to seal");
                }
                _plogInfo[writeToWAL].second = resp;
                return _client.create();
            })
            .then([&] (auto&& response){
                auto& [status, resp] = response;
                if (!status.is2xxOK()) {
                    throw std::runtime_error("unable to create plog");
                }
                
                auto plogId = resp;
                if (writeToWAL){
                    Payload temp_payload([] { return Binary(4096); });
                    temp_payload.write(_plogInfo[writeToWAL].second);
                    temp_payload.write(resp);
                    
                    return _write(std::move(temp_payload), false)
                    .then([&] (){
                        _plogInfo[writeToWAL].first = plogId;
                        _plogInfo[writeToWAL].second = 0;
                        return _write(std::move(backup_payload), writeToWAL);
                    });
                }
                else{
                    return _cpo.UpdateMetadataStreamLog(Deadline<>(_cpo_timeout()), _logStreamName, _plogInfo[0].second, resp)
                    .then([&] (auto&& response){
                        auto& [status, resp] = response;
                        if (!status.is2xxOK()) {
                            throw std::runtime_error("unable to update MSL to CPO");
                        }
                        return _write(std::move(backup_payload), writeToWAL);
                    });
                }
            });
        }
    });
}


String LogStream::_generateStreamName(){
    String logStreamName = "LogStream_0123456789";
    std::mt19937 g(std::rand());
    std::shuffle(logStreamName.begin()+logStreamName.size()-10, logStreamName.end(), g);
    return logStreamName;
}

} // k2
