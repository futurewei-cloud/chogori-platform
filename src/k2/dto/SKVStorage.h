/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

#include <k2/appbase/AppEssentials.h>

#include "Log.h"

namespace k2 {
namespace dto {

// The data actually stored by the Chogori storage node, and returned by a read request
struct SKVStorage {
    // Bitmap of fields that are excluded because they are optional or this is for a partial update
    std::vector<bool> excludedFields;
    Payload fieldData;
    uint32_t schemaVersion = 0;

    SKVStorage share() {
        return SKVStorage {
            excludedFields,
            fieldData.shareAll(),
            schemaVersion
        };
    }
    SKVStorage copy() {
        return SKVStorage {
            excludedFields,
            fieldData.copy(),
            schemaVersion
        };
    }

    K2_PAYLOAD_FIELDS(excludedFields, fieldData, schemaVersion);
    K2_DEF_FMT(SKVStorage, excludedFields, schemaVersion);
};

} // ns dto
} // ns k2
