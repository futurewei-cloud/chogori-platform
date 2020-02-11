/******************************************************************************
**
** Copyright (C) 2019 Ivan Pinezhaninov <ivan.pinezhaninov@gmail.com>
**
** This file is part of the IntervalTree - Red-Black balanced interval tree
** which can be found at https://github.com/IvanPinezhaninov/IntervalTree/.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
** IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
** DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
** OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
** THE USE OR OTHER DEALINGS IN THE SOFTWARE.
**
******************************************************************************/

#include "catch.hpp"
#include "intervaltree.hpp"
#include "random.h"
#include "timer.h"

#include <iostream>
#include <random>

static const size_t SIZE = 1000;


TEST_CASE("Benchmark")
{
    const IntervalVector &intervals = Random::createIntervals(SIZE);
    const IntervalTree tree = IntervalTree(intervals);
    const IntervalVector &searchIntervals = Random::createIntervals(SIZE);


    // Vector: Insert intervals
    {
        const Timer::Time &start = Timer::now();

        IntervalVector insertingIntervals;

        for (const Interval &interval : intervals) {
            insertingIntervals.push_back(interval);
        }

        Timer::printTimeElapsed(start, SIZE, "Vector: Insert intervals");
    }


    // Tree: Insert intervals
    {
        IntervalTree tree;

        const Timer::Time &start = Timer::now();

        for (const Interval &interval : intervals) {
            tree.insert(interval);
        }

        Timer::printTimeElapsed(start, SIZE, "Tree:   Insert intervals");
    }


    std::cout << std::endl;


    // Vector: Remove intervals
    {
        IntervalVector removingIntervals = intervals;

        auto rand = std::default_random_engine {};
        std::shuffle(removingIntervals.begin(), removingIntervals.end(), rand);

        const Timer::Time &start = Timer::now();

        for (const Interval &interval : intervals) {
            auto it = std::find(removingIntervals.begin(), removingIntervals.end(), interval);
            assert(it != removingIntervals.cend());
            removingIntervals.erase(it);
        }

        Timer::printTimeElapsed(start, SIZE, "Vector: Remove intervals");
    }


    // Tree: Remove intervals
    {
        IntervalTree tree = IntervalTree(intervals);

        const Timer::Time &start = Timer::now();

        for (const Interval &interval : intervals) {
            tree.remove(interval);
        }

        Timer::printTimeElapsed(start, SIZE, "Tree:   Remove intervals");
    }


    std::cout << std::endl;


    // Vector: Find overlapping intervals
    {
        const Timer::Time &start = Timer::now();

        for (const Interval &searchInterval : searchIntervals) {
            IntervalVector foundIntervals;
            foundIntervals.reserve(size_t(intervals.size() * Intervals::VECTOR_RESERVE_RATE));
            for (const Interval &interval : intervals) {
                if (searchInterval.low <= interval.high && interval.low <= searchInterval.high) {
                    foundIntervals.push_back(interval);
                }
            }
        }


        Timer::printTimeElapsed(start, SIZE, "Vector: Find overlapping intervals");
    }


    // Tree: Find overlapping intervals
    {
        const Timer::Time &start = Timer::now();

        for (const Interval &interval : searchIntervals) {
            tree.findOverlappingIntervals(interval);
        }

        Timer::printTimeElapsed(start, SIZE, "Tree:   Find overlapping intervals");
    }


    std::cout << std::endl;


    // Vector: Find inner intervals
    {
        const Timer::Time &start = Timer::now();

        for (const Interval &searchInterval : searchIntervals) {
            IntervalVector foundIntervals;
            foundIntervals.reserve(size_t(intervals.size() * Intervals::VECTOR_RESERVE_RATE));
            for (const Interval &interval : intervals) {
                if (searchInterval.low <= interval.low && interval.high <= searchInterval.high) {
                    foundIntervals.push_back(interval);
                }
            }
        }


        Timer::printTimeElapsed(start, SIZE, "Vector: Find inner intervals");
    }


    // Tree: Find inner intervals
    {
        const Timer::Time &start = Timer::now();

        for (const Interval &interval : searchIntervals) {
            tree.findInnerIntervals(interval);
        }

        Timer::printTimeElapsed(start, SIZE, "Tree:   Find inner intervals");
    }


    std::cout << std::endl;


    // Vector: Find outer intervals
    {
        const Timer::Time &start = Timer::now();

        for (const Interval &searchInterval : searchIntervals) {
            IntervalVector foundIntervals;
            foundIntervals.reserve(size_t(intervals.size() * Intervals::VECTOR_RESERVE_RATE));
            for (const Interval &interval : intervals) {
                if (interval.low <= searchInterval.low && searchInterval.high <= interval.high) {
                    foundIntervals.push_back(interval);
                }
            }
        }

        Timer::printTimeElapsed(start, SIZE, "Vector: Find outer intervals");
    }


    // Tree: Find outer intervals
    {
        const Timer::Time &start = Timer::now();

        for (const Interval &interval : searchIntervals) {
            tree.findOuterIntervals(interval);
        }

        Timer::printTimeElapsed(start, SIZE, "Tree:   Find outer intervals");
    }


    std::cout << std::endl;


    // Vector: Find intervals contain point
    {
        const Timer::Time &start = Timer::now();

        for (int i = 0; i < int(SIZE); ++i) {
            IntervalVector foundIntervals;
            foundIntervals.reserve(size_t(intervals.size() * Intervals::VECTOR_RESERVE_RATE));
            for (const Interval &interval : intervals) {
                if (interval.low <= i && i <= interval.high) {
                    foundIntervals.push_back(interval);
                }
            }
        }

        Timer::printTimeElapsed(start, SIZE, "Vector: Find intervals contain point");
    }


    // Tree: Find intervals contain point
    {
        const Timer::Time &start = Timer::now();

        for (int i = 0; i < int(SIZE); ++i) {
            tree.findIntervalsContainPoint(i);
        }

        Timer::printTimeElapsed(start, SIZE, "Tree:   Find intervals contain point");
    }
}
