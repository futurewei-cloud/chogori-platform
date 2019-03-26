#pragma once

#include <x86intrin.h>

static uint64_t myRDTSC()
{
    uint64_t result;
    uint32_t tmp;
    // Compiler-only barrier
    asm volatile("" : : : "memory");
    // Serializing version of rdtsc
    result = __rdtscp(&tmp);
    // Compiler-only barrier
    asm volatile("" : : : "memory");
    return result;
}

static uint64_t toNanoseconds(uint64_t cycles, double cyclesPerSec)
{
    return (uint64_t)(1e09 * (double)(cycles) / cyclesPerSec + 0.5);
}

static uint64_t toMicroseconds(uint64_t cycles, double cyclesPerSec)
{
    return toNanoseconds(cycles, cyclesPerSec) / 1000;
}
