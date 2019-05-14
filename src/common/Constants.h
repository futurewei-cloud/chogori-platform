#pragma once

#include <cstdint>

namespace k2
{

class Constants
{
public:
    static constexpr uint8_t MaxCountOfPartitionsPerNode = 4;
    static constexpr uint8_t MaxCountCountOfNodesPerPool = 64;

    // what verbs we support
    enum class NodePoolMsgVerbs: uint8_t {
        PARTITION_MESSAGE = 100
    };
};

}   //  namespace k2
