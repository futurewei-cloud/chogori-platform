/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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
    RandomContext(int seed=0) : _generator(seed) {};

    RandomContext(int seed, std::vector<double> prob) : _generator(seed), _discreteDis(prob.begin(),prob.end()) {};

    uint32_t UniformRandom(uint32_t min, uint32_t max)
    {
        std::uniform_int_distribution<> dist(min, max);
        return dist(_generator);
    }

    k2::String RandomString(uint32_t length)
    {
        k2::String str(k2::String::initialized_later{}, length);

        for (uint32_t i=0; i<length; ++i) {
            str[i] = (char)UniformRandom(33, 126); // Non-whitespace and non-control ASCII characters
        }

        return str;
    }

    std::set<uint32_t> RandomSetInt(uint32_t num, uint32_t max, uint32_t min = 1){
        // Create a set to hold the random numbers
        std::set<uint32_t> randNos;

        // Generate the random numbers
        while(randNos.size() != num)
        {
            randNos.insert(UniformRandom(min,max));
        }

        return randNos;
    }

    uint64_t BiasedInt() // will be used for selecting operations
    {
        return _discreteDis(_generator);
    }

private:
    std::mt19937 _generator;
    std::discrete_distribution<> _discreteDis;
};

class RandomGenerator {
public:
    virtual uint64_t getValue() = 0;
    virtual ~RandomGenerator() = default;
};

class UniformGenerator : public RandomGenerator {
public:
    UniformGenerator(int max, int min = 0, int seed=0) : _generator(seed), _dist(min,max) {};

     uint64_t getValue() override {
         return _dist(_generator);
     }

private:
    std::mt19937 _generator;
    std::uniform_int_distribution<> _dist;
};

