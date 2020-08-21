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

#include "Collection.h"

#pragma once
// This file contains DTOs for K2 Persistence Service

namespace k2 {
namespace dto {

// Request to create a Partition Group
struct PartitionGroupCreateRequest {
    String partitionName;
    std::vector<String> plogServerEndpoints;
    K2_PAYLOAD_FIELDS(partitionName, plogServerEndpoints);
};

struct PartitionGroupCreateResponse {
    K2_PAYLOAD_EMPTY;
};

// Request to obtain the Partition Group Map
struct PartitionMapGetRequest {
    uint32_t offset;
    K2_PAYLOAD_FIELDS(offset);
};

struct PartitionMapGetResponse {
    std::unordered_map<String, std::vector<String>> partitionMap;
    K2_PAYLOAD_FIELDS(partitionMap);
};

}  // namespace dto
}  // namespace k2
