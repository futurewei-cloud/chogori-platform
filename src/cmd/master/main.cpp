// std
#include <iostream>
// seastar
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
// k2
#include "common/scheduler/seastar_scheduler.hh"
#include "common/scheduler/roundrobin_scheduler.hh"

/**
 * Service entry point.
 */
int main(int argc, char** argv) {
    try {
        // create seastar application
        seastar::app_template app;

        // start the application
        app.run(argc, argv, [] {
            // start the event loop
            return seastar::do_with(RoundRobinScheduler(), false, [] (auto& scheduler, bool& stop) {
                using namespace std::chrono_literals;

                // stop the scheduler after 10s
                seastar::sleep(std::chrono::seconds(10)).then([&stop] {
                    stop = true;
                });

                // run the scheduler
                return SeastarScheduler::run(scheduler, stop);
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
