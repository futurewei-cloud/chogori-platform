#pragma once

// seastar
#include <seastar/core/seastar.hh>
#include <seastar/core/gate.hh>
// k2
#include "ISchedulable.h"

namespace k2
{

//
// Scheduler interface.
//
class IScheduler {
// The RootScheduler needs access to this class in order to set the gate pointer during registration.
friend class RootScheduler;

private:
    // Indicates if the scheduler should stop execution.
    bool _stopFlag = false;
    // Set by the RootScheduler when the Scheduler is registered. It is invoked by the RootScheduler during termination. 
    seastar::shared_ptr<seastar::gate> _gatePtr;

protected:
    // 
    // Returns the gate shared pointer.
    //
    seastar::shared_ptr<seastar::gate> getGate() {
        return _gatePtr;
    }

    //
    // Indicates if the scheduler implementation should stop execution.
    //
    bool shouldStop() {
        return _stopFlag;
    }

public:
    //
    // Invoked when the scheduler implementation is terminated.
    //
    virtual ~IScheduler() {
        stop();
    };

    // 
    // Starts the scheduler.
    //
    virtual seastar::future<> start() =0;
    
    // 
    // Stops the scheduler.
    //
    virtual void stop() {
        _stopFlag = true;
    };

}; // class Scheduler

} // namespace k2