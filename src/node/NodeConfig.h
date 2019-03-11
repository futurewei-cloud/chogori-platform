#pragma once

#include "Module.h"

namespace k2
{

//
//  Static configuration of the node. TODO: loading, etc.
//
class NodeConfig
{
public:
    static std::chrono::nanoseconds getTaskProcessingIterationMaxExecutionTime() { return std::chrono::nanoseconds(10000); }

    static IModule* getModule() { return nullptr; }
};

}   //  namespace k2
