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

#include "timer.h"

#include <iomanip>
#include <iostream>


Timer::Time Timer::now()
{
    return std::chrono::steady_clock::now();
}


void Timer::printTimeElapsed(const Timer::Time &start, size_t iterationsCount, const std::string &text)
{
    using Ns = std::chrono::nanoseconds;
    const Time &end = std::chrono::steady_clock::now();
    const double totalDuration = std::chrono::duration_cast<Ns>(end - start).count() * 0.000001;
    const double iterationDuration = totalDuration / iterationsCount;
    std::cout << "[" <<std::fixed << std::setw(7)
              << std::setprecision(3) << std::setfill(' ') << totalDuration << " ("
              << std::setprecision(4) << iterationDuration << ") ms]: " << text << std::endl;
}
