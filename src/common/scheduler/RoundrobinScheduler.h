#pragma once

// k2
#include "Scheduler.h"

/**
 * Simple round roubin scheduler implemetation.
 */
class RoundRobinScheduler : public Scheduler
{
public:
    RoundRobinScheduler(){
        // empty
    };
    virtual ~RoundRobinScheduler(){
        // empty
    };

    // run the scheduler for one iteration
    virtual void run();
};
