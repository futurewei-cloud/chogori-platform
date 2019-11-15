#pragma once

// seastar
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
// k2:transport
#include <transport/RPCDispatcher.h>
// k2:client
#include <client/IClient.h>
// k2:executor
#include "IServiceLauncher.h"
#include "IService.h"


namespace k2
{

class EventLoopService: public IService
{

public:
    // Service launcher
    class Launcher: public IServiceLauncher
    {
    private:
        std::unique_ptr<seastar::distributed<EventLoopService>> _pDistributed;
        std::function<uint64_t(client::IClient&)> _runInLoop;
        client::IClient& _rClient;

    public:
        Launcher(std::function<uint64_t(client::IClient&)> runInLoop, client::IClient& rClient)
        : _runInLoop(runInLoop)
        , _rClient(rClient)
        {
            // EMPTY
        }

        virtual seastar::future<> init(k2::RPCDispatcher::Dist_t& rDispatcher) {
            if(nullptr == _pDistributed.get()) {
                // create the service in the Seastar context
                _pDistributed = std::make_unique<seastar::distributed<EventLoopService>>();
            }

            return _pDistributed->start(std::ref(rDispatcher), _runInLoop, std::ref(_rClient));
        }

        virtual seastar::future<> stop() {
            if(nullptr == _pDistributed.get()) {

                return seastar::make_ready_future<>();
            }

            return _pDistributed->stop().then([&] {
                _pDistributed.release();

               return seastar::make_ready_future<>();
            });
        }

        virtual seastar::future<> start() {

           return _pDistributed->invoke_on_all(&EventLoopService::start);
        }
    };

private:
    const std::chrono::microseconds _minDelay; // the minimum amount of time before calling the next application loop
    k2::RPCDispatcher::Dist_t& _rDispatcher;
    seastar::semaphore _sempahore;
    std::function<uint64_t(client::IClient&)> _runInLoop;
    client::IClient& _rClient;
    // class defined
    bool _stopFlag = false; // stops the service
public:
    EventLoopService(k2::RPCDispatcher::Dist_t& rDispatcher, std::function<uint64_t(client::IClient&)> runInLoop, client::IClient& rClient)
    : _minDelay(std::chrono::microseconds(5))
    , _rDispatcher(rDispatcher)
    , _sempahore(seastar::semaphore(1))
    , _runInLoop(runInLoop)
    , _rClient(rClient)
    {
        // EMPTY
    }

    virtual seastar::future<> start()
    {
        return seastar::with_semaphore(_sempahore, 1, [&] {
            return seastar::do_until([&] { return _stopFlag; }, [&] {
                // execute client loop
                const long int timeslice = _runInLoop(_rClient);
                const auto delay = (timeslice < _minDelay.count()) ? _minDelay : std::chrono::microseconds(timeslice);

                return seastar::sleep(std::move(delay));
            });
        })
        .or_terminate();

    }

    virtual seastar::future<> stop()
    {
        _stopFlag = true;

        return seastar::with_semaphore(_sempahore, 1, [&] {

            return seastar::make_ready_future<>();
        });
    }


}; // class EventLoopService

}; // namespace k2
