#pragma once

// std
#include <iostream>
// seastar
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
// k2
#include "Scheduler.h"

/**
 * Seastar wrapper class which invokes a scheduler implementation.
 */
class SeastarScheduler
{
public:
    SeastarScheduler(){
        // left empty
    };
    ~SeastarScheduler(){
        // left empty
    };

    /**
     * Executes the provided scheduler until the stop flag is set.
     */
    static seastar::future<> run(Scheduler &scheduler, bool &stop);
};
