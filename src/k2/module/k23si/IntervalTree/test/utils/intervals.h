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

#ifndef TEST_INTERVALS_H
#define TEST_INTERVALS_H

#include "common.h"


namespace Test {

Interval interval();

IntervalVector innerIntervals();

IntervalVector outerIntervals();

IntervalVector leftIntervals();

IntervalVector rightIntervals();

IntervalVector leftOverlappingIntervals();

IntervalVector rightOverlappingIntervals();

IntervalVector boundaryIntervals();

IntervalVector leftBoundaryIntervals();

IntervalVector rightBoundaryIntervals();

IntervalVector compositeIntervals(const IntervalVector &a, const IntervalVector &b);

} // namespace Test

#endif // TEST_INTERVALS_H
