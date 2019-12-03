#pragma once

namespace k2
{

class IService: public seastar::weakly_referencable<IService>
{

public:
    virtual seastar::future<> start()= 0;
    virtual seastar::future<> stop()= 0;

     virtual ~IService()
     {
         // EMPTY
     }

}; // IService

}; // namespace k2
