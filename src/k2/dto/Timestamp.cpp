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

#include "Timestamp.h"

namespace k2 {
namespace dto {

Timestamp::CompareResult Timestamp::compareUncertain(const Timestamp& other) const {
    if (tsoId == other.tsoId) {
        return endCount < other.endCount ? LT :
        (endCount > other.endCount ? GT : EQ);
    }
    else {
        return endCount < other.endCount - other.startDelta ? LT :
            (endCount - startDelta > other.endCount ? GT : UN);
    }
}

Timestamp::CompareResult Timestamp::compareCertain(const Timestamp& other) const {
    if (endCount != other.endCount){
        return endCount < other.endCount ? LT : GT;
    }
    else {
        return tsoId < other.tsoId ? LT :
            (tsoId > other.tsoId ? GT : EQ);
    }
}

Timestamp Timestamp::operator-(const Duration d) const {
    uint64_t cnt = nsec(d).count();
    if (endCount <= cnt + startDelta) {
        // our clock hasn't started that far back. Return minimum value
        return Timestamp{.endCount=startDelta, .tsoId=tsoId, .startDelta=startDelta};
    }
    return Timestamp{.endCount=endCount - cnt, .tsoId=tsoId, .startDelta=startDelta};
}

Timestamp Timestamp::operator+(const Duration d) const {
    return Timestamp{.endCount=endCount + nsec(d).count(),.tsoId= tsoId, .startDelta=startDelta};
}

size_t Timestamp::hash() const {
    return hash_combine(endCount, startDelta, tsoId);
}

bool Timestamp::operator==(const Timestamp& other) const noexcept {
    // certain and uncertain have the same behavior when the timestamps are equal
    return compareUncertain(other) == Timestamp::EQ;
}

bool Timestamp::operator!=(const Timestamp& other) const noexcept {
    return !operator==(other);
}

void Timestamp::minEq(const Timestamp& other) {
    if (compareCertain(other) == GT) {
        operator=(other);
    }
}

void Timestamp::maxEq(const Timestamp& other) {
    if (compareCertain(other) == LT) {
        operator=(other);
    }
}

Timestamp Timestamp::min(const Timestamp& other) const {
    return compareCertain(other) == Timestamp::LT ? (*this) : other;
}

Timestamp Timestamp::max(const Timestamp& other) const {
    return compareCertain(other) == Timestamp::GT ? (*this) : other;
}

} // ns dto
} // ns k2
