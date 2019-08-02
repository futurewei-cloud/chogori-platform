// std
#include <iostream>
#include <string>
// seastar
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/gate.hh>
// k2
#include "common/scheduler/RootScheduler.h"
#include "common/scheduler/LoopScheduler.h"
#include "common/scheduler/TcpService.h"

using namespace k2;

seastar::shared_ptr<RootScheduler> rootScheduler;

//
// Node entry point.
//
int main(int argc, char** argv) {
    try {
        // create seastar application
        seastar::app_template app;

        // start the application
        app.run(argc, argv, [] {
            std::cout << "Starting service..." << std::endl;
            rootScheduler = seastar::make_shared<RootScheduler>();

            // at exit, terminate the root scheduler.
            seastar::engine().at_exit([] {
                std::cout << "Stopping service..." << std::endl;

                return rootScheduler->close();
            });

            // TODO: Remove this code once we have a way to send a signal to terminate the service.
            seastar::sleep(std::chrono::seconds(10)).then([] {
                rootScheduler->stop();

                return seastar::make_ready_future<>();
            }).ignore_ready_future();

            return seastar::async([] {
                // create schedulers
                rootScheduler->createSchedulingGroup("default", 1000).get();
                seastar::shared_ptr<LoopScheduler> loopScheduler = seastar::make_shared<LoopScheduler>(seastar::make_shared<TcpService>());
                rootScheduler->registerScheduler(loopScheduler);
                // start schedulers
                rootScheduler->start().get();
            });
        });
    } catch (...) {
        std::cerr << "Failed to start the application; error:" << std::current_exception() << std::endl;

        // error
        return 1;
    }

    // success
    return 0;
}
