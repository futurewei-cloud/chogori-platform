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

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>

#include <k2/tso/client/Client.h>
#include <k2/tso/service/Service.h>

using namespace seastar;

namespace k2 {
namespace log {
inline thread_local k2::logging::Logger tsoapp("k2::tso_app");
}
using namespace dto;

class sampleTSOApp {
public:
    sampleTSOApp(){};

    seastar::future<> gracefulStop() {
        K2LOG_I(log::tsoapp, "stop");
        return seastar::make_ready_future<>();
    };

    seastar::future<> start() {
        K2LOG_I(log::tsoapp, "start");
        return seastar::sleep(2s)
        .then([this] {
            K2LOG_I(log::tsoapp, "Getting new timestamp");
            return AppBase().getDist<k2::tso::TSOClient>().local().getTimestamp();
        })
        .then([this](auto&& ts) {
            K2LOG_I(log::tsoapp, "Received response with timestamp{}", ts);
            return seastar::sleep(100us);
        })
        .then([this] {
            K2LOG_I(log::tsoapp, "getting second timestamp");
             return AppBase().getDist<k2::tso::TSOClient>().local().getTimestamp();
        })
        .then([](auto&& ts) {
            K2LOG_I(log::tsoapp, "Received second response with timestamp{}", ts);
        });
    }
};
}

int main(int argc, char **argv)
{
    k2::App app("TSOSampleApp");
    app.addOptions()
    ("tso_endpoint", bpo::value<k2::String>()->default_value("tcp+k2rpc://127.0.0.1:9000"), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");

    app.addApplet<k2::tso::TSOClient>();
    app.addApplet<k2::tso::TSOService>();
    app.addApplet<k2::sampleTSOApp>();

    return app.start(argc, argv);
}

