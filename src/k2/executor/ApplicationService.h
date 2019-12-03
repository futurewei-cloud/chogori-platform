#pragma once

// std
#include <memory>
// seastar
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
// k2:transport
#include <k2/transport/RPCDispatcher.h>
// k2:executor
#include "IServiceLauncher.h"
#include "IService.h"
#include "IApplication.h"

namespace k2
{

template <class T>
class ApplicationService: public IService
{
public:
    // Service launcher
    class Launcher: public IServiceLauncher
    {
    private:
        std::unique_ptr<seastar::distributed<ApplicationService<T>>> _pDistributed;
        IApplication<T>& _rApplication;
        std::unique_ptr<T> _pContext;

    public:
        Launcher(IApplication<T>& rApplication, T& rContext)
        : _rApplication(rApplication)
        , _pContext(std::move(rContext.newInstance()))
        {
            // EMPTY
        }

        virtual seastar::future<> init(k2::RPCDispatcher::Dist_t& rDispatcher) {
            (void)rDispatcher;
            if(nullptr == _pDistributed.get()) {
                // create the service in the Seastar context
                _pDistributed = std::make_unique<seastar::distributed<ApplicationService>>();
            }

            return _pDistributed->start(std::ref(_rApplication), std::ref(*_pContext));
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

           return _pDistributed->invoke_on_all(&ApplicationService<T>::start);
        }
    };

protected:
    const std::chrono::microseconds _minDelay; // the minimum amount of time before calling the next application loop
    seastar::semaphore _sempahore;
    std::unique_ptr<IApplication<T>> _pApplication;
    // class defined
    bool _stopFlag = false; // stops the service

public:
    ApplicationService(IApplication<T>& rApplication, T& rContext)
    : _minDelay(std::chrono::microseconds(5))
    , _sempahore(seastar::semaphore(1))
    , _pApplication(std::move(rApplication.newInstance()))
    {
        _pApplication->onInit(std::move(rContext.newInstance()));
    }

    virtual seastar::future<> start()
    {
        _pApplication->onStart();

         return seastar::with_semaphore(_sempahore, 1, [&] {
            return seastar::do_until([&] { return _stopFlag; }, [&] {
                // execute event loop
                const long int timeslice = _pApplication->eventLoop();
                const auto delay = (timeslice < _minDelay.count()) ? _minDelay : std::chrono::microseconds(timeslice);

                return seastar::sleep(std::move(delay));
            });
        })
        .or_terminate();
    }

    virtual seastar::future<> stop()
    {
        _stopFlag = true;
        _pApplication->onStop();

        return seastar::with_semaphore(_sempahore, 1, [&] {

            return seastar::make_ready_future<>();
        });
    }

}; // class ApplicationService

}; // namespace k2
