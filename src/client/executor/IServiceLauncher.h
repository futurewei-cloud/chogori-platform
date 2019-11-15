#pragma once

namespace k2
{

class IServiceLauncher 
{
public:
    virtual seastar::future<> init(k2::RPCDispatcher::Dist_t& dispatcher)= 0;
    virtual seastar::future<> start()= 0;
    virtual seastar::future<> stop()= 0;

     virtual ~IServiceLauncher()
     {
         // EMPTY
     }

}; // IServiceLauncher

}; // namespace k2
