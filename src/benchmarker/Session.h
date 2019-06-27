#pragma once

// std
#include <random>
#include <string>
// k2b
#include <benchmarker/generator/Generator.h>
#include "KeySpace.h"

namespace k2
{
namespace benchmarker
{

class Session
{
public:
    KeySpace _keySpace;
    KeySpace::iterator _it;

    Session(KeySpace&& keySpace)
    : _keySpace(std::move(keySpace))
    , _it(_keySpace.end())
    {
        // empty
    }

};

};
};
