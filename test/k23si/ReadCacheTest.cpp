/*
*   (C)opyright Futurewei Technologies Inc, 2020
*/

#define CATCH_CONFIG_MAIN

#include <k2/module/k23si/read_cache.h>
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

