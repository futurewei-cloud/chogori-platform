#pragma once

// std
#include <random>
#include <k2/common/Common.h>
// k2b
#include "Generator.h"

namespace k2
{
namespace benchmarker
{

class RandomNumberGenerator: public Generator
{
protected:
    std::mt19937_64 _rand;
    uint64_t _seed;

public:
    RandomNumberGenerator()
    {
        _seed = Clock::now().time_since_epoch().count();
        _rand.seed(_seed);
    }

    RandomNumberGenerator(uint64_t seed)
    {
        _seed = seed;
        _rand.seed(_seed);
    }

protected:
    virtual std::string next()
    {
        return std::to_string(_rand());
    }
};

};
};
