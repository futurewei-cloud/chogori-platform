#pragma once

#include "Module.h"

namespace k2
{
class ISchedulingPlatform
{
public:
    //
    //  Return id of the current node
    //
    virtual uint64_t getCurrentNodeId() = 0;

    //
    //  Execute callback after some delay
    //
    virtual void delay(std::chrono::microseconds delayTime, std::function<void()>&& callback) = 0;
};

}   //  namespace k2
