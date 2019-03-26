#pragma once

/**
 * Scheduler interface.
 */
class Scheduler
{
public:
    virtual ~Scheduler() {}
    virtual void run() = 0;
};