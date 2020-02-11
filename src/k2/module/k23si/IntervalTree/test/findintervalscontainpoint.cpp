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

static const int centralPoint = 450;
static const int leftBoundaryPoint = 400;
static const int rightBoundaryPoint = 500;

TEST_CASE("Find intervals contain point")
{
    SECTION("Empty tree")
    {
        const IntervalTree tree;
        const auto result = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE(tree.isEmpty());
        REQUIRE(result.empty());
    }


    SECTION("Boundary interval central point")
    {
        const auto tree = IntervalTree(Test::boundaryIntervals());
        const auto result = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE_FALSE(result.empty());

        auto boundaryIntervals = Test::boundaryIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), boundaryIntervals.cbegin()));
    }


    SECTION("Boundary interval left boundary point")
    {
        const auto tree = IntervalTree(Test::boundaryIntervals());

        auto result = tree.findIntervalsContainPoint(leftBoundaryPoint, true);
        REQUIRE_FALSE(result.empty());

        auto boundaryIntervals = Test::boundaryIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), boundaryIntervals.cbegin()));

        result = tree.findIntervalsContainPoint(leftBoundaryPoint, false);
        REQUIRE(result.empty());
    }


    SECTION("Boundary interval right boundary point")
    {
        const auto tree = IntervalTree(Test::boundaryIntervals());

        auto result = tree.findIntervalsContainPoint(rightBoundaryPoint, true);
        REQUIRE_FALSE(result.empty());

        auto boundaryIntervals = Test::boundaryIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), boundaryIntervals.cbegin()));

        result = tree.findIntervalsContainPoint(rightBoundaryPoint, false);
        REQUIRE(result.empty());
    }


    SECTION("Outer and boundary intervals central point")
    {
        const auto intervals = Test::compositeIntervals(Test::outerIntervals(), Test::boundaryIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE_FALSE(result.empty());
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), intervals.cbegin()));
    }


    SECTION("Outer and boundary intervals left boundary point")
    {
        const auto intervals = Test::compositeIntervals(Test::outerIntervals(), Test::boundaryIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findIntervalsContainPoint(leftBoundaryPoint);

        REQUIRE_FALSE(result.empty());
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), intervals.cbegin()));
    }


    SECTION("Outer and boundary intervals right boundary point")
    {
        const auto intervals = Test::compositeIntervals(Test::outerIntervals(), Test::boundaryIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findIntervalsContainPoint(rightBoundaryPoint);

        REQUIRE_FALSE(result.empty());
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), intervals.cbegin()));
    }


    SECTION("Left intervals")
    {
        const auto tree = IntervalTree(Test::leftIntervals());
        const auto result = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE(result.empty());
    }


    SECTION("Left and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::leftIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE_FALSE(result.empty());

        auto innerIntervals = Test::innerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), innerIntervals.cbegin()));
    }


    SECTION("Right intervals")
    {
        const auto tree = IntervalTree(Test::rightIntervals());
        const auto result = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE(result.empty());
    }


    SECTION("Right and inner intervals")
    {
        const auto intervals = Test::compositeIntervals(Test::rightIntervals(), Test::innerIntervals());
        const auto tree = IntervalTree(intervals);
        const auto result = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE_FALSE(result.empty());

        auto innerIntervals = Test::innerIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), innerIntervals.cbegin()));
    }


    SECTION("Left overlapping intervals and left boundary point")
    {
        const auto tree = IntervalTree(Test::leftOverlappingIntervals());
        const auto result = tree.findIntervalsContainPoint(leftBoundaryPoint);

        REQUIRE_FALSE(result.empty());

        auto leftOverlappingIntervals = Test::leftOverlappingIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), leftOverlappingIntervals.cbegin()));
    }


    SECTION("Right overlapping intervals and right boundary point")
    {
        const auto tree = IntervalTree(Test::rightOverlappingIntervals());
        const auto result = tree.findIntervalsContainPoint(rightBoundaryPoint);

        REQUIRE_FALSE(result.empty());

        auto rightOverlappingIntervals = Test::rightOverlappingIntervals();
        REQUIRE(std::is_permutation(result.cbegin(), result.cend(), rightOverlappingIntervals.cbegin()));
    }


    SECTION("Intervals contain point order")
    {
        const auto tree = IntervalTree(Test::innerIntervals());
        const auto intervals = tree.findIntervalsContainPoint(centralPoint);

        REQUIRE_FALSE(intervals.empty());

        Interval previousInterval(0, 0);
        for (auto interval : intervals) {
            REQUIRE(previousInterval < interval);
        }
    }
}
