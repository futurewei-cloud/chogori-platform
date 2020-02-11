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
#include "common.h"

#include <list>
#include <set>

using IntervalList = std::list<Interval>;
using IntervalSet = std::set<Interval>;


TEST_CASE("Interval constructor for regular values")
{
    {
        Interval interval(1, 2);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(0 == interval.value);
    }

    {
        Interval interval(2, 1);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(0 == interval.value);
    }

    {
        Interval interval(1, 2, 3);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(3 == interval.value);
    }

    {
        Interval interval(2, 1, 3);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(3 == interval.value);
    }
}


TEST_CASE("Interval constructor for pair")
{
    {
        Interval interval(std::make_pair(1, 2));
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(0 == interval.value);
    }

    {
        Interval interval(std::make_pair(2, 1));
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(0 == interval.value);
    }

    {
        Interval interval(std::make_pair(1, 2), 3);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(3 == interval.value);
    }

    {
        Interval interval(std::make_pair(2, 1), 3);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(3 == interval.value);
    }
}


TEST_CASE("Interval constructor for tuple")
{
    {
        Interval interval(std::make_tuple(1, 2));
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(0 == interval.value);
    }

    {
        Interval interval(std::make_tuple(2, 1));
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(0 == interval.value);
    }

    {
        Interval interval(std::make_tuple(1, 2), 3);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(3 == interval.value);
    }

    {
        Interval interval(std::make_tuple(2, 1), 3);
        REQUIRE(1 == interval.low);
        REQUIRE(2 == interval.high);
        REQUIRE(3 == interval.value);
    }
}


TEST_CASE("IntervalTree constructor for list container")
{
    IntervalList intervals
    {
        { 1, 2 },
        { 3, 4 }
    };

    IntervalTree intervalTree(intervals);

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}


TEST_CASE("IntervalTree constructor for set container")
{
    IntervalSet intervals
    {
        { 1, 2 },
        { 3, 4 }
    };

    IntervalTree intervalTree(intervals);

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}


TEST_CASE("IntervalTree constructor for vector container")
{
    IntervalVector intervals
    {
        { 1, 2 },
        { 3, 4 }
    };

    IntervalTree intervalTree(intervals);

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}


TEST_CASE("IntervalTree constructor for list container iterators")
{
    IntervalList intervals
    {
        { 1, 2 },
        { 3, 4 }
    };

    IntervalTree intervalTree(intervals.begin(), intervals.end());

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}


TEST_CASE("IntervalTree constructor for set container iterators")
{
    IntervalSet intervals
    {
        { 1, 2 },
        { 3, 4 }
    };

    IntervalTree intervalTree(intervals.begin(), intervals.end());

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}


TEST_CASE("IntervalTree constructor for vector container iterators")
{
    IntervalVector intervals
    {
        { 1, 2 },
        { 3, 4 }
    };

    IntervalTree intervalTree(intervals.begin(), intervals.end());

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}
