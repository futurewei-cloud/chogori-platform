#pragma once

#include <cstdint>

namespace k2
{

class Constants
{
public:
    static constexpr uint8_t MaxCountOfPartitionsPerNode = 4;
    static constexpr uint8_t MaxCountCountOfNodesPerPool = 64;
};

} //  namespace k2
