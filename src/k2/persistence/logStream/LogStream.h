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

    
    seastar::future<> init(String cpo_url, String persistenceClusrerName);

    // create a log stream
    seastar::future<> create();

    // write data to the log stream
    seastar::future<> write(Payload payload);

    // read data from the log stream
    seastar::future<std::vector<Payload> > read(String logStreamName);

private:
    CPOClient _cpo;
    PlogClient _client;
    String _logStreamName;

    std::vector<std::pair<String, uint32_t>> _plogInfo;
    bool _logStreamCreate = false;
    

    seastar::future<> _write(Payload payload, bool writeToWAL);

    seastar::future<std::vector<Payload> > _readContent(std::vector<Payload> payloads);
    // generate the log stream name
    // TODO: change the method to generate the random plog id later
    String _generateStreamName();

    ConfigDuration _cpo_timeout {"cpo_timeout", 1s};
};

} // k2
