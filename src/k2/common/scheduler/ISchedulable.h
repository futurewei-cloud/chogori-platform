#pragma once

// seastar
#include <seastar/core/seastar.hh>

namespace k2 
{

//
// The Schedulable interface defines the contract with the Scheduler and represents a unit of work which can be carried out
// by the scheduler. The Scheduler will periodically invoke the Schedulable when is its turn for execution.
class ISchedulable {
public:
    //
    // Destractor; invoked when the Schedulable is terminated.
    //
    virtual ~ISchedulable() {};
    
    //
    // Periodically invoked by the Scheduler during execution. The Scheduler will wait on the returned future to complete before scheduling the next task.
    //
    virtual seastar::future<> run() =0;

}; // class Schedulable

} // namespace k2