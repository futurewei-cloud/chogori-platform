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

#include <k2/tso/client/tso_clientlib.h>
#include <k2/tso/service/TSOService.h>

using namespace seastar;

namespace k2 {
namespace log {
inline thread_local k2::logging::Logger tsoapp("k2::tso_app");
}
using namespace dto;

// debugging print, time point (nano accurate) to a stream
// TODO we can use https://en.cppreference.com/w/cpp/chrono/system_clock/to_stream here, but it is a C++20 feature
inline void TimePointToStream(int64_t nanosec, char buffer[100])
{
    auto microsec = nanosec/1000;
    nanosec -= microsec*1000;
    auto millis = microsec/1000;
    microsec -= millis*1000;
    auto secs = millis/1000;
    millis -= secs*1000;
    auto mins = (secs/60);
    secs -= (mins*60);
    auto hours = (mins/60);
    mins -= (hours*60);
    auto days = (hours/24);
    hours -= (days*24);

    std::snprintf(buffer, 100, "%04ld:%02ld:%02ld:%02ld.%03ld.%03ld.%03ld", days, hours, mins, secs, millis, microsec, nanosec);
}

class sampleTSOApp
{
public:
    sampleTSOApp(){};

    seastar::future<> gracefulStop() {
        K2LOG_I(log::tsoapp, "stop");
        return seastar::make_ready_future<>();
    };

    void start()
    {
        K2LOG_I(log::tsoapp, "start");
        (void)seastar::sleep(2s)
        .then([this]
        {
            K2LOG_I(log::tsoapp, "Getting 2 timestamps async in loop at least 100us apart from each other");

            auto curSteadyTime = Clock::now();
            char timeBuffer[100];
            TimePointToStream(nsec_count(curSteadyTime), timeBuffer);
            K2LOG_I(log::tsoapp, "Issuing first 100us apart TS at local request time: {}", timeBuffer);
            (void) AppBase().getDist<k2::TSO_ClientLib>().local().getTimestampFromTSO(curSteadyTime)
                .then([this](auto&& timestamp)
                {
                    char timeBufferS[100];
                    char timeBufferE[100];
                    TimePointToStream(timestamp.tStartTSECount(), timeBufferS);
                    TimePointToStream(timestamp.tEndTSECount(), timeBufferE);
                    K2LOG_I(log::tsoapp, "got first 100us apart TS value:[{}:{}], TSOId:{}", timeBufferS, timeBufferE, timestamp.tsoId());
                    (void) seastar::sleep(100us)
                    .then([this]
                    {
                        TimePoint curSteadyTime = Clock::now();
                        char timeBuffer[100];
                        TimePointToStream(nsec_count(curSteadyTime), timeBuffer);
                        K2LOG_I(log::tsoapp, "Issuing second 100us apart TS at local request time: {}", timeBuffer);
                        (void) AppBase().getDist<k2::TSO_ClientLib>().local().getTimestampFromTSO(curSteadyTime)
                        .then([](auto&& ts)
                        {
                            char timeBufferS[100];
                            char timeBufferE[100];
                            TimePointToStream(ts.tStartTSECount(), timeBufferS);
                            TimePointToStream(ts.tEndTSECount(), timeBufferE);
                            K2LOG_I(log::tsoapp, "got second 100us apart TS value:[{}:{}] TSOId:{}", timeBufferS,timeBufferE, ts.tsoId());
                        });
                    });
                });
        });
    };
};
}

int main(int argc, char **argv)
{
    k2::App app("TSOSampleApp");
    app.addOptions()
    ("tso_endpoint", bpo::value<k2::String>()->default_value("tcp+k2rpc://127.0.0.1:9000"), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'");

    app.addApplet<k2::TSO_ClientLib>();
    app.addApplet<k2::TSOService>();
    app.addApplet<k2::sampleTSOApp>();

    return app.start(argc, argv);
}

