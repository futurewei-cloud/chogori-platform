#pragma once

namespace k2
{

class IApplication
{
public:
    virtual void runOnce()= 0;

    virtual ~IApplication() 
    {
        // empty
    }
    
}; // class IApplication

}; // namespace k2