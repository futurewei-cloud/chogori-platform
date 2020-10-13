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
#include <k2/dto/Persistence.h>
#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>

namespace k2
{



class PlogServer
{
private:
    // the maximum size of each plog
    const static uint32_t PLOG_MAX_SIZE = 16 * 1024 * 1024;

    // each PlogPage is a plog. It uses payload to store the data, and contains the sealed and offest as metadata
    struct PlogPage {
        PlogPage(){
            sealed=false;
            offset=0;
            payload = Payload(([] { return Binary(PLOG_MAX_SIZE); }));
        }

        bool sealed;
        uint32_t offset;
        Payload payload; 
    };

    // a map to store all the plogs based on plog id
    std::unordered_map<String, PlogPage> _plogMap;

    //handle the create request
    seastar::future<std::tuple<Status, dto::PlogCreateResponse>>
    _handleCreate(dto::PlogCreateRequest&& request);

    //handle the read request
    seastar::future<std::tuple<Status, dto::PlogAppendResponse>>
    _handleAppend(dto::PlogAppendRequest&& request);

    //handle the read request
    seastar::future<std::tuple<Status, dto::PlogReadResponse>>
    _handleRead(dto::PlogReadRequest&& request);

    //handle the seal request
    seastar::future<std::tuple<Status, dto::PlogSealResponse>>
    _handleSeal(dto::PlogSealRequest&& request);

    //handle the seal request
    seastar::future<std::tuple<Status, dto::PlogInfoResponse>>
    _handleInfo(dto::PlogInfoRequest&& request);
    
public:
     PlogServer();
    ~PlogServer();

    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();

};//  class PlogServer

} //  namespace k2