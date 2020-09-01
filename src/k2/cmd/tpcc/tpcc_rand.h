/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include <random>
#include <cstdint>
#include <cmath>
#include <cstring>

class RandomContext {
public:
    RandomContext(int seed=0) : _generator(seed) {
        std::uniform_int_distribution<> dist255(0, 255);
        _C_A255 = dist255(_generator);
        std::uniform_int_distribution<> dist1023(0, 1023);
        _C_A1023 = dist1023(_generator);
        std::uniform_int_distribution<> dist8191(0, 8191);
        _C_A8191 = dist8191(_generator);
    }

    uint32_t UniformRandom(uint32_t min, uint32_t max)
    {
        std::uniform_int_distribution<> dist(min, max);
        return dist(_generator);
    }

    float UniformRandom(uint32_t min, uint32_t max, uint32_t decimalPoints)
    {
        if (decimalPoints == 0) {
            return (float)UniformRandom(min, max);
        }

        float factor = pow(10, decimalPoints);
        min = min * factor;
        max = max * factor;
        return UniformRandom(min, max) / factor;
    }

    uint32_t NonUniformRandom(uint32_t A, uint32_t min, uint32_t max)
    {
        uint32_t C;
        if (A == 255) {
            C = _C_A255;
        } else if (A == 1023) {
            C = _C_A1023;
        } else if (A == 8191) {
            C = _C_A8191;
        } else {
            throw 0;
        }

        return (((UniformRandom(0, A) | UniformRandom(min, max)) + C) % (max - min + 1)) + min;
    }

    k2::String RandomString(uint32_t min, uint32_t max)
    {
        uint32_t length = UniformRandom(min, max);
        k2::String str(k2::String::initialized_later{}, length);

        for (uint32_t i=0; i<length; ++i) {
            str[i] = (char)UniformRandom(33, 126); // Non-whitespace and non-control ASCII characters
        }

        return str;
    }

    k2::String RandomNumericString(uint32_t min, uint32_t max)
    {
        uint32_t length = UniformRandom(min, max);
        k2::String str(k2::String::initialized_later{}, length);

        for (uint32_t i=0; i<length; ++i) {
            str[i] = (char)UniformRandom(48, 57); // 0-9 ASCII
        }

        return str;
    }

    k2::String RandomZipString()
    {
        k2::String zip = RandomNumericString(10, 10);

        for (int i=4; i<9; ++i) {
            zip[i] = '1';
        }
        zip[9] = 0;

        return zip;
    }

private:
    std::mt19937 _generator;
    // Random constants used by TPC-C
    uint32_t _C_A255;
    uint32_t _C_A1023;
    uint32_t _C_A8191;
};
