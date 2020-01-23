#pragma once

#include <random>
#include <cstdint>
#include <cmath>
#include <cstring>

class RandomContext {
public:
    RandomContext(int seed) : _generator(seed) {
        std::uniform_int_distribution<> dist255(0, 255);
        C_A255 = dist(_generator);
        std::uniform_int_distribution<> dist1023(0, 1023);
        C_A1023 = dist(_generator);
        std::uniform_int_distribution<> dist255(0, 8191);
        C_A8191 = dist(_generator);
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
            C = _C_8191;
        } else {
            throw 0;
        }

        return (((UniformRandom(0, A) | UniformRandom(min, max)) + C) % (max - min + 1)) + min;
    }

    void RandomString(uint32_t min, uint32_t max, char str[])
    {
        uint32_t length = UniformRandom(min, max);
        bzero(str, length+1);

        for (int i=0; i<length; ++i) {
            str[i] = (char)UniformRandom(33, 126); // Non-whitespace and non-control ASCII characters
        }
    }

    void RandomNumericString(uint32_t min, uint32_t max, char str[])
    {
        uint32_t length = UniformRandom(min, max);
        memset(str, 0, length+1);

        for (int i=0; i<length; ++i) {
            str[i] = (char)UniformRandom(48, 57); // 0-9 ASCII
        }
    }

    void RandomZipString(char zip[])
    {
        RandomNumericString(4, 4, zip);
        for (int i=4; i<9; ++i) {
            zip[i] = '1';
        }
        zip[9] = 0;
    }

private:
    std::mt19937 _generator;
    // Random constants used by TPC-C
    uint32_t _C_A255;
    uint32_t _C_A1023;
    uint32_t _C_A8191;
};
