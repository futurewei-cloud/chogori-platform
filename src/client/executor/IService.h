#pragma once

#include "transport/RPCDispatcher.h"

namespace k2
{

class IService {
public:
    virtual seastar::future<> init(k2::RPCDispatcher::Dist_t& dispatcher)= 0;
    virtual seastar::future<> start()= 0;
    virtual seastar::future<> stop()= 0;

     virtual ~IService()
     {
         // EMPTY
     }
     
}; // IService

}; // namespace k2
