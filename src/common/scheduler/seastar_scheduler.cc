#include "seastar_scheduler.hh"

seastar::future<> SeastarScheduler::run(Scheduler& scheduler, bool& stop) {
    return seastar::do_until([&stop] {return stop;}, [&scheduler] {
        using namespace std::chrono_literals;
        
        // run the provided scheduler
        scheduler.run();

        return seastar::make_ready_future<>();
    });
}