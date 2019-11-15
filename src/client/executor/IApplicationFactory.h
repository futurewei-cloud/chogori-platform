#pragma once

namespace k2
{

class IApplicationFactory
{
    virtual std::unique_ptr<IApplication> newInstance()= 0;

    virtual ~IApplicationFactory()
    {
        // empty
    }

}; // class IApplicationFactory

}; // namespace k2