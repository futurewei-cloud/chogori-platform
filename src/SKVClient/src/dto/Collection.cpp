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

#include <algorithm>
#include <string>

#include "Collection.h"

namespace skv::http::dto {

int Key::compare(const Key& o) const noexcept {
    auto scomp = schemaName.compare(o.schemaName);
    if (scomp != 0) {
        return scomp;
    }

    auto pkcomp = partitionKey.compare(o.partitionKey);
    if (pkcomp == 0) {
        // if the partition keys are equal, return the comparison of the range keys
        return rangeKey.compare(o.rangeKey);
    }
    return pkcomp;
}

bool Key::operator<(const Key& o) const noexcept {
    return compare(o) < 0;
}
bool Key::operator<=(const Key& o) const noexcept {
    return compare(o) <= 0;
}
bool Key::operator>(const Key& o) const noexcept {
    return compare(o) > 0;
}
bool Key::operator>=(const Key& o) const noexcept {
    return compare(o) >= 0;
}
bool Key::operator==(const Key& o) const noexcept {
    return compare(o) == 0;
}
bool Key::operator!=(const Key& o) const noexcept {
    return compare(o) != 0;
}
size_t Key::hash() const noexcept {
    return hash_combine(schemaName, partitionKey, rangeKey);
}

// hash value
size_t KeyRangeVersion::hash() const noexcept {
    return hash_combine(startKey, endKey, pvid);
}

// comparison for unordered containers
bool KeyRangeVersion::operator==(const KeyRangeVersion& o) const noexcept {
    return startKey == o.startKey && endKey == o.endKey && pvid == o.pvid;
}

bool KeyRangeVersion::operator!=(const KeyRangeVersion& o) const noexcept {
    return !operator==(o);
}

bool KeyRangeVersion::operator<(const KeyRangeVersion& o) const noexcept {
    return startKey < o.startKey;
}

bool PVID::operator==(const PVID& o) const noexcept {
    return id == o.id && rangeVersion == o.rangeVersion && assignmentVersion == o.assignmentVersion;
}

bool PVID::operator!=(const PVID& o) const noexcept {
    return !operator==(o);
}

// hash value
size_t PVID::hash() const noexcept {
    return hash_combine(id, rangeVersion, assignmentVersion);
}
}  // namespace skv::http::dto
