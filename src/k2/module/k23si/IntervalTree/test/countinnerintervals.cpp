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


TEST_CASE("Count inner intervals")
{
    SECTION("Empty tree")
    {
        const IntervalTree tree;
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(tree.isEmpty());
        REQUIRE(0 == count);
    }


    SECTION("Boundary interval")
    {
        const auto tree = IntervalTree(Test::boundaryIntervals());

        auto count = tree.countInnerIntervals(Test::interval(), true);
        REQUIRE(count == Test::boundaryIntervals().size());

        count = tree.countInnerIntervals(Test::interval(), false);
        REQUIRE(0 == count);
    }


    SECTION("Outer intervals")
    {
        const auto tree = IntervalTree(Test::outerIntervals());
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(0 == count);
    }


    SECTION("Outer and boundary intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::outerIntervals(), Test::boundaryIntervals());
        const auto tree = IntervalTree(intervals);
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == Test::boundaryIntervals().size());
    }


    SECTION("Inner intervals")
    {
        const auto tree = IntervalTree(Test::innerIntervals());
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == Test::innerIntervals().size());
    }


    SECTION("Inner and boundary intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::innerIntervals(), Test::boundaryIntervals());
        const auto tree = IntervalTree(intervals);
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == intervals.size());
    }


    SECTION("Outer and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::outerIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == Test::innerIntervals().size());
    }


    SECTION("Left intervals")
    {
        const auto tree = IntervalTree(Test::leftIntervals());
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(0 == count);
    }


    SECTION("Left and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::leftIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == Test::innerIntervals().size());
    }


    SECTION("Right intervals")
    {
        const auto tree = IntervalTree(Test::rightIntervals());
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(0 == count);
    }


    SECTION("Right and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::rightIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == Test::innerIntervals().size());
    }


    SECTION("Left overlapping intervals")
    {
        const auto tree = IntervalTree(Test::leftOverlappingIntervals());
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(0 == count);
    }


    SECTION("Left overlapping and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::leftOverlappingIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == Test::innerIntervals().size());
    }


    SECTION("Right overlapping intervals")
    {
        const auto tree = IntervalTree(Test::rightOverlappingIntervals());
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(0 == count);
    }


    SECTION("Right overlapping and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::rightOverlappingIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto count = tree.countInnerIntervals(Test::interval());

        REQUIRE(count == Test::innerIntervals().size());
    }
}
