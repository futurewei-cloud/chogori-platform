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

#define CATCH_CONFIG_RUNNER

#include "catch.hpp"
#include "common.h"
#include "random.h"


TEST_CASE("Intervals with the same boundaries and different values")
{
    IntervalVector intervals
    {
        { 1, 2, 1 },
        { 1, 2, 2 }
    };

    IntervalTree intervalTree;

    for (auto interval : intervals) {
        REQUIRE(intervalTree.insert(interval));
    }

    for (auto interval : intervals) {
        REQUIRE_FALSE(intervalTree.insert(interval));
    }

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}


TEST_CASE("Const types")
{
    using Interval = Intervals::Interval<const int, const size_t>;
    using IntervalTree = Intervals::IntervalTree<const int, const size_t>;
    using IntervalVector = std::vector<Interval>;

    IntervalVector intervals
    {
        { 100, 200 },
        { 101, 201 }
    };

    IntervalTree intervalTree;

    for (auto interval : intervals) {
        REQUIRE(intervalTree.insert(interval));
    }

    auto treeIntervals = intervalTree.intervals();

    REQUIRE_FALSE(treeIntervals.empty());
    REQUIRE(std::is_permutation(treeIntervals.cbegin(), treeIntervals.cend(), intervals.cbegin()));
}


int main(int argc, char* argv[]) {
    Catch::Session session;

    const auto res = session.applyCommandLine(argc, argv);
    if (0 != res) {
        return res;
    }

    const auto seed = session.config().rngSeed();
    if (0 != seed) {
        Random::setRndGeneratorSeed(seed);
    }

    return session.run(argc, argv);
}
