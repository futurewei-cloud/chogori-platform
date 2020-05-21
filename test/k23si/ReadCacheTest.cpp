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

#define CATCH_CONFIG_MAIN

#include <k2/module/k23si/ReadCache.h>
#include "catch2/catch.hpp"

SCENARIO("Basic read cache tests") {
    auto cache = ReadCache<uint64_t, uint64_t>(10, 5);

    // Not in cache, should return min value set at creation
    uint64_t t = cache.checkInterval(15, 15);
    REQUIRE(t == 10);

    // Basic insert/check test
    cache.insertInterval(15, 15, 12);
    t = cache.checkInterval(15, 15);
    REQUIRE(t == 12);

    // Overlapping intervals, should get highest timestamp
    cache.insertInterval(15, 25, 18);
    cache.insertInterval(10, 20, 17);
    t = cache.checkInterval(15, 15);
    REQUIRE(t == 18);

    // Fill up cache, inserts not in timestamp order
    cache.insertInterval(50, 51, 23);
    cache.insertInterval(40, 45, 20);

    // Not in cache, should return min value set at creation
    t = cache.checkInterval(150, 150);
    REQUIRE(t == 10);

    // Add one more over cache size limit, forcing LRU eviction
    cache.insertInterval(0, 5, 25);
    t = cache.checkInterval(0, 0);
    REQUIRE(t == 25);

    // Not in cache, should be new min of 12
    t = cache.checkInterval(150, 150);
    REQUIRE(t == 12);

    // Exact interval as previous inseration, should be no LRU eviction
    cache.insertInterval(0, 5, 33);
    t = cache.checkInterval(0, 0);
    REQUIRE(t == 33);
    t = cache.checkInterval(150, 150);
    REQUIRE(t == 12);

    // Add one more causing eviction, evicted interval (15, 25, 18) was not in timestamp order
    cache.insertInterval(80, 82, 34);
    t = cache.checkInterval(10, 10);
    REQUIRE(t == 18);

    // Add one more causing eviction, evicted interval (10, 20, 17) was not in timestamp order
    cache.insertInterval(90, 92, 35);
    t = cache.checkInterval(150, 150);
    REQUIRE(t == 18);

    // Add exact interval with lower timestamp
    cache.insertInterval(90, 92, 32);
    t = cache.checkInterval(90, 90);
    REQUIRE(t == 35);
}

