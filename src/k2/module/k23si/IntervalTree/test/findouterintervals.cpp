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
#include "intervals.h"


TEST_CASE("Find outer intervals")
{
    SECTION("Empty tree")
    {
        const IntervalTree tree;
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE(tree.isEmpty());
        REQUIRE(result.empty());
    }


    SECTION("Boundary interval")
    {
        const auto tree = IntervalTree(Test::boundaryIntervals());

        auto result = tree.findOuterIntervals(Test::interval(), true);
        REQUIRE_FALSE(result.empty());

        auto boundaryIntervals = Test::boundaryIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), boundaryIntervals.cbegin()));

        result = tree.findOuterIntervals(Test::interval(), false);
        REQUIRE(result.empty());
    }


    SECTION("Outer intervals")
    {
        const auto tree = IntervalTree(Test::outerIntervals());
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());

        auto outerIntervals = Test::outerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), outerIntervals.cbegin()));
    }


    SECTION("Outer and boundary intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::outerIntervals(), Test::boundaryIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), intervals.cbegin()));
    }


    SECTION("Inner intervals")
    {
        const auto tree = IntervalTree(Test::innerIntervals());
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE(result.empty());
    }


    SECTION("Inner and boundary intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::innerIntervals(), Test::boundaryIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());

        auto boundaryIntervals = Test::boundaryIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), boundaryIntervals.cbegin()));
    }


    SECTION("Outer and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::outerIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());

        auto outerIntervals = Test::outerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), outerIntervals.cbegin()));
    }


    SECTION("Left intervals")
    {
        const auto tree = IntervalTree(Test::leftIntervals());
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE(result.empty());
    }


    SECTION("Left and outer intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::leftIntervals(), Test::outerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());

        auto outerIntervals = Test::outerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), outerIntervals.cbegin()));
    }


    SECTION("Right intervals")
    {
        const auto tree = IntervalTree(Test::rightIntervals());
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE(result.empty());
    }


    SECTION("Right and outer intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::rightIntervals(), Test::outerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());

        auto outerIntervals = Test::outerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), outerIntervals.cbegin()));
    }


    SECTION("Left overlapping intervals")
    {
        const auto tree = IntervalTree(Test::leftOverlappingIntervals());
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE(result.empty());
    }


    SECTION("Left overlapping and outer intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::leftOverlappingIntervals(), Test::outerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());

        auto outerIntervals = Test::outerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), outerIntervals.cbegin()));
    }


    SECTION("Right overlapping intervals")
    {
        const auto tree = IntervalTree(Test::rightOverlappingIntervals());
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE(result.empty());
    }


    SECTION("Right overlapping and outer intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::rightOverlappingIntervals(), Test::outerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(result.empty());

        auto outerIntervals = Test::outerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), outerIntervals.cbegin()));
    }


    SECTION("Outer intervals order")
    {
        const auto tree = IntervalTree(Test::outerIntervals());
        const auto intervals = tree.findOuterIntervals(Test::interval());

        REQUIRE_FALSE(intervals.empty());

        Interval previousInterval(0, 0);
        for (auto interval : intervals) {
            REQUIRE(previousInterval < interval);
        }
    }
}
