#pragma once

// seastar
#include <seastar/core/seastar.hh>
// k2
#include "IScheduler.h"

namespace k2
{

//
// A Scheduler implementation which runs the provided Schedulable in a continuous looop.
//
class LoopScheduler: public IScheduler {
private:
    seastar::shared_ptr<ISchedulable> _schedulablePtr;

public:
    LoopScheduler(seastar::shared_ptr<ISchedulable> schedulable) {
        _schedulablePtr = schedulable;
    };

protected:
    seastar::future<> start() {
        if(getGate()==nullptr) {
            throw std::runtime_error("Attempted to run an unregistered scheduler!");
        }

        return seastar::with_gate(*getGate(), [this] {
            return seastar::do_until([this] {return shouldStop();}, [this] {
                return _schedulablePtr->run();
            });
        });
    }

}; // class LoopScheduler

} // namespace k2