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

#include "K23SI.h"
namespace skv::http::dto {

bool K23SI_MTR::operator==(const K23SI_MTR& o) const {
    return timestamp == o.timestamp && priority == o.priority;
}
bool K23SI_MTR::operator!=(const K23SI_MTR& o) const {
    return !(operator==(o));
}

size_t K23SI_MTR::hash() const {
    return hash_combine(timestamp, priority);
}

void Query::setFilterExpression(dto::expression::Expression&& root) {
    request.filterExpression = std::move(root);
}

void Query::setReverseDirection(bool reverseDirection) {
    request.reverseDirection = reverseDirection;
}

void Query::setIncludeVersionMismatch(bool includeVersionMismatch) {
    request.includeVersionMismatch = includeVersionMismatch;
}

void Query::setLimit(int32_t limit) {
    request.recordLimit = limit;
}

void Query::addProjection(const String& fieldName) {
    request.projection.push_back(fieldName);
    checkKeysProjected();
}

void Query::addProjection(const std::vector<String>& fieldNames) {
    for (const String& name : fieldNames) {
        request.projection.push_back(name);
    }
    checkKeysProjected();
}

void Query::checkKeysProjected() {
    keysProjected = false;
    for (uint32_t idx : schema->partitionKeyFields) {
        String& name = schema->fields[idx].name;
        bool found = false;
        for (const String& projected : request.projection) {
            if (projected == name) {
                found = true;
            }
        }

        if (!found) {
            return;
        }
    }
    for (uint32_t idx : schema->rangeKeyFields) {
        String& name = schema->fields[idx].name;
        bool found = false;
        for (const String& projected : request.projection) {
            if (projected == name) {
                found = true;
            }
        }

        if (!found) {
            return;
        }
    }

    keysProjected = true;
}

bool Query::isDone() {
    return done;
}

} // ns dto
