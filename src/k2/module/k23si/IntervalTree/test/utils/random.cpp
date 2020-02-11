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

#include "random.h"

#include <ctime>
#include <random>


static const size_t MIN_VALUE = 1;
static const size_t MAX_VALUE = 1000000;
static const size_t MIN_RANGE = 1;
static const size_t MAX_RANGE = 1000;

static std::mt19937 gen(static_cast<unsigned long>(time(nullptr)));


void Random::setRndGeneratorSeed(unsigned int seed)
{
    gen.seed(seed);
}


int rand(int min, int max)
{
    assert(min < max);
    std::uniform_int_distribution<> dis(min, max);
    return dis(gen);
}


Interval Random::createInterval()
{
  const int low = rand(MIN_VALUE, MAX_VALUE);
  const int high = rand(low + int(MIN_RANGE), low + int(MAX_RANGE));
  return {low, high};
}


IntervalVector Random::createIntervals(size_t treeSize)
{
    IntervalVector intervals;
    intervals.reserve(treeSize);

    while (intervals.size() < treeSize) {
        const Interval &interval = createInterval();
        if (intervals.cend() == std::find(intervals.cbegin(), intervals.cend(), interval)) {
            intervals.push_back(interval);
        }
    }

    return intervals;
}


IntervalTree Random::createTree(size_t treeSize)
{
    return IntervalTree(createIntervals(treeSize));
}
