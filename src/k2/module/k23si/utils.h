/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

namespace k2 {
// Class to update metrics (i) count of operation and (ii) latency of operation
class OperationLatencyReporter{
    public:
        OperationLatencyReporter(uint64_t& ops, k2::ExponentialHistogram& latencyHist): _ops(ops), _latencyHist(latencyHist){
            #if defined GET_LATENCY || defined FORCE_GET_LATENCY
                _startTime = k2::Clock::now(); // record start time to calculate latency of operation
            #endif
        }

        // update operation count and latency metrics
        void report(){
            _ops++;
            #if defined GET_LATENCY || defined FORCE_GET_LATENCY
                auto end = k2::Clock::now();
                auto dur = end - _startTime;
                _latencyHist.add(dur);
            #endif
        }

    private:
        k2::TimePoint _startTime;
        uint64_t& _ops;
        k2::ExponentialHistogram& _latencyHist;
};

}
