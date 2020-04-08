#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>

#include <k2/tso/client_lib/tso_clientlib.h>
#include <k2/tso/service/TSOService.h>

using namespace seastar;

namespace k2 {

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
    sampleTSOApp(k2::App& baseApp) : _baseApp(baseApp) {};

    seastar::future<> stop() 
    {
        K2INFO("stop");
        return seastar::make_ready_future<>();
    };

    void start() 
    {
        K2INFO("start");
        (void)seastar::sleep(2s)
        .then([this] 
        {
            K2INFO("Getting 2 timestamps async in loop at least 100us apart from each other");

            auto curSteadyTime = Clock::now();
            char timeBuffer[100];
            TimePointToStream(curSteadyTime.time_since_epoch().count(), timeBuffer);
            K2INFO("Issuing first 100us apart TS at local request time:" << timeBuffer);
            (void) _baseApp.getDist<k2::TSO_ClientLib>().local().GetTimestampFromTSO(curSteadyTime)
                .then([this](auto&& timestamp)
                {
                    char timeBufferS[100];
                    char timeBufferE[100];
                    TimePointToStream(timestamp.tStartTSECount(), timeBufferS);
                    TimePointToStream(timestamp.tEndTSECount(), timeBufferE);
                    K2INFO("got first 100us apart TS value:{[" << timeBufferS <<":"<< timeBufferE<<"] TSOId:" <<timestamp.tsoId());
                    (void) seastar::sleep(100us)
                    .then([this]
                    {
                        TimePoint curSteadyTime = Clock::now();
                        char timeBuffer[100];
                        TimePointToStream(curSteadyTime.time_since_epoch().count(), timeBuffer);
                        K2INFO("Issuing second 100us apart TS at local request time:" << timeBuffer);
                        (void) _baseApp.getDist<k2::TSO_ClientLib>().local().GetTimestampFromTSO(curSteadyTime)
                        .then([](auto&& ts)
                        {
                            char timeBufferS[100];
                            char timeBufferE[100];
                            TimePointToStream(ts.tStartTSECount(), timeBufferS);
                            TimePointToStream(ts.tEndTSECount(), timeBufferE);
                            K2INFO("got second 100us apart TS value:{[" << timeBufferS <<":"<< timeBufferE<<"] TSOId:" <<ts.tsoId());
                        });
                    });
                });
        /*})
        .then([this] {
            K2INFO("Getting 1000 timestamps async in loop continuously from each other");
            seastar::distributed<k2::TSO_ClientLib>& tso_client_dist =  _baseApp.getDist<k2::TSO_ClientLib>();
            auto tso_clientlib = tso_client_dist.local();

            for (int i = 0; i < 10; i++)
            {
                TimePoint curSteadyTime = Clock::now();
                K2INFO("Issuing continous TS #"<<i<<" at local request time:" << curSteadyTime.time_since_epoch().count());
                (void) tso_clientlib.GetTimestampFromTSO(curSteadyTime)
                    .then([idx=i](auto&& timestamp)
                    {
                        K2INFO("got continous TS #"<<idx<<" TS value:" << timestamp.TStartTSECount() <<":"<< timestamp.TEndTSECount());
                    });
            }
        })
        .then([] {
            K2INFO("Finished issuing all the request, sleeping for 5 second.");
            (void) seastar::sleep(5s)
            .then([]{K2INFO("5 second sleep done, all requests should be finished as well.");});
           */
        });
    };
private:
   k2::App& _baseApp;
};
}

int main(int argc, char **argv) 
{
    const std::string endpointUrl = "tcp+k2rpc://127.0.0.1:9000";
    k2::App app;
    app.addApplet<k2::TSO_ClientLib>(endpointUrl, 1s);
    app.addApplet<k2::TSOService>(seastar::ref(app));
    app.addApplet<k2::sampleTSOApp>(seastar::ref(app));

    return app.start(argc, argv);
}
