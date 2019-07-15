#pragma once


#include <seastar/net/api.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>

#include <node/Node.h>
#include <node/ISchedulingPlatform.h>

namespace k2
{

class SeastarApp
{
private:
    constexpr void registerServices() {}
protected:
    seastar::app_template app;
    std::vector<std::function<seastar::future<>()>> initializers;
    std::vector<std::function<seastar::future<>()>> finalizers;
public:

    SeastarApp& registerInitializer(std::function<seastar::future<>()>&& initFunction)
    {
        initializers.push_back(std::move(initFunction));
        return *this;
    }

    template<typename ServiceT, typename... StartArgs>
    SeastarApp& registerService(seastar::distributed<ServiceT>& service, StartArgs&&... startArgs)
    {
        finalizers.push_back([&] { return service.stop(); });
        initializers.push_back([&service, args = std::make_tuple(std::forward<StartArgs>(startArgs)...)]() mutable
            {
                return std::apply([&service](auto&& ... args) mutable
                {
                    return service.start(std::forward<StartArgs>(args)...);
                }, std::move(args));
            });

        return *this;
    }

    template<typename ServiceT, typename... StartArgs>
    SeastarApp& registerNonDistributedService(ServiceT& service, StartArgs&&... startArgs)
    {
        finalizers.push_back([&] { return service.Stop(); });
        initializers.push_back([&service, args = std::make_tuple(std::forward<StartArgs>(startArgs)...)]() mutable
        {
            return std::apply([&service](auto&& ... args) mutable
            {
                return service.Start(std::forward<StartArgs>(args)...);
            }, std::move(args));
        });

        return *this;
    }

    template<typename ServiceT, typename... StartArgs>
    SeastarApp& startOnCores(seastar::distributed<ServiceT>& service, StartArgs&&... startArgs)
    {
        initializers.push_back([&service, args = std::make_tuple(std::forward<StartArgs>(startArgs)...)]() mutable
            {
                return std::apply([&service](auto&& ... args) mutable
                {
                    return service.invoke_on_all(&ServiceT::start, std::forward<StartArgs>(args)...);
                }, std::move(args));
            });

        return *this;
    }

    template<typename ServiceT, typename... ArgsT>
    SeastarApp& registerServices(seastar::distributed<ServiceT>& service, ArgsT&... args)
    {
        registerService(service);
        registerServices(args...);

        return *this;
    }

    SeastarApp& registerInfoLog(std::string logMessage)
    {
        return registerInitializer([logMessage = std::move(logMessage)]() mutable
            {
                K2INFO(std::move(logMessage));
                return seastar::make_ready_future<>();
            });
    }

    int run(int argc, const char** argv)
    {
        return app.run_deprecated(argc, (char**)argv, [&, this]
            {
                for(auto&& finalizer : finalizers)
                    seastar::engine().at_exit(std::move(finalizer));
                finalizers.clear();

                auto runFuture = seastar::make_ready_future<>();
                for(auto&& initializer : initializers)
                    runFuture = runFuture.then(std::move(initializer));
                initializers.clear();

                return runFuture;
            });
    }
};

} // namespace k2
