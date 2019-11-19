#pragma once

// std
#include <memory>
// k2:executor
#include "IApplicationContext.h"

namespace k2
{

template <class T>
class IApplication
{
public:
    virtual uint64_t eventLoop()= 0;
    virtual void onInit(std::unique_ptr<T>)= 0;
    virtual void onStart()= 0;
    virtual void onStop()= 0;
    virtual std::unique_ptr<IApplication> newInstance()= 0;

    virtual ~IApplication()
    {
        // empty
    }

}; // class IApplication

}; // namespace k2
