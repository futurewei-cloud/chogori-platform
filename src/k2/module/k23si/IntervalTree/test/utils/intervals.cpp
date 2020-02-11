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

#include "intervals.h"


Interval Test::interval()
{
    return {400, 500};
}


IntervalVector Test::innerIntervals()
{
    return IntervalVector
    {
        { 410, 490 },
        { 420, 480 },
        { 420, 470 },
        { 430, 470 },
        { 440, 460 }
    };
}


IntervalVector Test::outerIntervals()
{
    return IntervalVector
    {
        { 390, 510 },
        { 380, 520 },
        { 380, 530 },
        { 370, 530 },
        { 360, 540 },
        { 350, 550 }
    };
}


IntervalVector Test::leftIntervals()
{
    return IntervalVector
    {
        { 290, 390 },
        { 270, 380 },
        { 270, 370 },
        { 260, 370 },
        { 250, 360 },
        { 240, 350 },
        { 230, 340 }
    };
}


IntervalVector Test::rightIntervals()
{
    return IntervalVector
    {
        { 510, 610 },
        { 520, 620 },
        { 520, 630 },
        { 530, 630 },
        { 540, 640 },
        { 550, 650 },
        { 560, 660 },
        { 570, 670 }
    };
}


IntervalVector Test::leftOverlappingIntervals()
{
    return IntervalVector
    {
        { 340, 440 },
        { 330, 430 },
        { 320, 420 },
        { 320, 410 },
        { 310, 410 },
        { 300, 400 }
    };
}


IntervalVector Test::rightOverlappingIntervals()
{
    return IntervalVector
    {
        { 460, 560 },
        { 470, 570 },
        { 480, 580 },
        { 480, 590 },
        { 490, 590 },
        { 500, 600 }
    };
}


IntervalVector Test::boundaryIntervals()
{
    return IntervalVector { interval() };
}


IntervalVector Test::leftBoundaryIntervals()
{
    return IntervalVector
    {
        { 100, 400 },
        { 200, 400 },
        { 300, 400 }
    };
}


IntervalVector Test::rightBoundaryIntervals()
{
    return IntervalVector
    {
        { 500, 600 },
        { 500, 700 },
        { 500, 800 }
    };
}


IntervalVector Test::compositeIntervals(const IntervalVector &a, const IntervalVector &b)
{
    IntervalVector composite = a;
    composite.reserve(a.size() + b.size());
    composite.insert(composite.end(), b.cbegin(), b.cend());
    return composite;
}
