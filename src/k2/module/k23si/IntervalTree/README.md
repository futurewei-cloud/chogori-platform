# IntervalTree

[![Linux Build Status](https://travis-ci.org/IvanPinezhaninov/IntervalTree.svg?branch=master)](https://travis-ci.org/IvanPinezhaninov/IntervalTree)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/github/IvanPinezhaninov/intervaltree?svg=true)](https://ci.appveyor.com/project/IvanPinezhaninov/intervaltree)
[![MIT License](https://img.shields.io/badge/license-mit-blue.svg?style=flat)](http://opensource.org/licenses/MIT)

## Overview

A red-black self-balancing interval tree C++11 implementation

## Usage

```c++
#include <iostream>
#include "intervaltree.hpp"

int main()
{
    // Create an interval tree
    Intervals::IntervalTree<int> intervalTree;

    // Insert intervals to the tree
    intervalTree.insert({ 20, 30 });
    intervalTree.insert({ 40, 60 });
    intervalTree.insert({ 70, 90 });
    intervalTree.insert({ 60, 70 });
    intervalTree.insert({ 40, 90 });
    intervalTree.insert({ 80, 90 });

    // Wanted interval and point
    const auto wantedInterval = Intervals::Interval<int>(50, 80);
    const auto wantedPoint = 50;

    // Find intervals
    const auto &overlappingIntervals = intervalTree.findOverlappingIntervals(wantedInterval);
    const auto &innerIntervals = intervalTree.findInnerIntervals(wantedInterval);
    const auto &outerIntervals = intervalTree.findOuterIntervals(wantedInterval);
    const auto &intervalsContainPoint = intervalTree.findIntervalsContainPoint(wantedPoint);

    // Print all intervals
    std::cout << "All intervals:" << std::endl;
    for (const auto &interval : intervalTree.intervals()) {
        std::cout << interval << std::endl;
    }
    std::cout << std::endl;

    // Print overlapping intervals
    std::cout << "Overlapping intervals for " << wantedInterval << ":" << std::endl;
    for (const auto &interval : overlappingIntervals) {
        std::cout << interval << std::endl;
    }
    std::cout << std::endl;

    // Print inner intervals
    std::cout << "Inner intervals for " << wantedInterval << ":" << std::endl;
    for (const auto &interval : innerIntervals) {
        std::cout << interval << std::endl;
    }
    std::cout << std::endl;

    // Print outer intervals
    std::cout << "Outer intervals for " << wantedInterval << ":" << std::endl;
    for (const auto &interval : outerIntervals) {
        std::cout << interval << std::endl;
    }
    std::cout << std::endl;

    // Print intervals contain a point
    std::cout << "Intervals contain a point value of " << wantedPoint << ":" << std::endl;
    for (const auto &interval : intervalsContainPoint) {
        std::cout << interval << std::endl;
    }

    return 0;
}
```
