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

#pragma once

#include <k2/transport/PayloadSerialization.h>
#include <seastar/core/sharded.hh>
#include <k2/transport/Payload.h>
#include <k2/transport/Status.h>
#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/persistence/plog_client/PlogClient.h>


namespace k2 {

class LogStream{
public:
    LogStream();
    ~LogStream();

    // initializate the plog client it holds
    seastar::future<> init(String cpo_url, String persistenceClusrerName);

    // create a log stream with a given name
    seastar::future<> create(String logStreamName);

    // write data to the log stream
    seastar::future<> write(Payload payload);

    // read all the data from this log stream
    seastar::future<std::vector<Payload> > read(String logStreamName);

private:
    // the maximum size of each plog
    const static uint32_t PLOG_MAX_SIZE = 16 * 1024 * 1024;
    // How many WAL plogs it will create in advance 
    const static uint32_t WAL_PLOG_POOL_SIZE = 16;
    // How many metadata plogs it will create in advance
    const static uint32_t METADATA_PLOG_POOL_SIZE = 4;

    CPOClient _cpo;
    PlogClient _client;
    String _logStreamName;

    std::vector<String> _walPlogPool; // The vector to store the created WAL plog Id
    std::vector<String> _metadataPlogPool; // the vector to store the created Metadata plog Id
    std::vector<std::pair<String, uint32_t>> _plogInfo; // _plogInfo[0] means the current metadata Plog Id and offset it holds, while _plogInfo[1] means the current WAL Plog Id and offset it holds,
    bool _logStreamCreate = false; // Whether this logstream has been created 

    // temporary data structure created for read operation
    std::vector<dto::MetadataElement> _streamLog; 
    // write data to the log stream. writeToWAL == 0 means write to the metadata log stream, while writeToWAL == 1 means write to the WAL
    seastar::future<> _write(Payload payload, uint32_t writeToWAL);
    // read the contents from WAL plogs
    seastar::future<std::vector<Payload> > _readContent(std::vector<Payload> payloads);


    ConfigDuration _cpo_timeout {"cpo_timeout", 1s};
};

} // k2
