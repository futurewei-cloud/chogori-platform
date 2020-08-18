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
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>


namespace k2 {

class PlogClient {
public:
    PlogClient();
    ~PlogClient();

    seastar::future<> gracefulStop();
    seastar::future<> start();


    void GetPlogServerEndpoint(String plog_server_url);

    seastar::future<> GetPlogServerUrls();


    seastar::future<std::tuple<Status, String>> create();

    seastar::future<std::tuple<Status, uint32_t>> append(String plogId, uint32_t offset, Payload payload);

    template <typename ClockT=Clock>
    seastar::future<std::tuple<Status, dto::PlogReadResponse>> read(Deadline<ClockT> deadline, String&& plogId, uint32_t offset, uint32_t size);

    template <typename ClockT=Clock>
    seastar::future<std::tuple<Status, dto::PlogSealResponse>> seal(Deadline<ClockT> deadline, String&& plogId, uint32_t offset);

private:
    std::unique_ptr<TXEndpoint> plog;
    std::vector<String> plog_server_endpoints;

    CPOClient _cpo;
    String generate_plogId();

    ConfigDuration get_plog_server_deadline{"get_plog_server_deadline", 1s};
    ConfigDuration plog_request_timeout{"plog_request_timeout", 100ms};
    ConfigVar<String> _cpo_url{"cpo_url", ""};

};

} // k2
