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
#include "Collection.h"
#include "Log.h"
// This file contains DTOs for K2 AssignmentManager

namespace k2 {
namespace dto {

// Request to create a collection
struct AssignmentCreateRequest {
    CollectionMetadata collectionMeta;
    Partition partition;
    std::vector<String> cpoEndpoints
    K2_PAYLOAD_FIELDS(collectionMeta, partition, cpoEndpoints);
};

// Response to AssignmentCreateRequest
struct AssignmentCreateResponse {
    Partition assignedPartition;
    K2_PAYLOAD_FIELDS(assignedPartition);
};

// Request to offload a collection
struct AssignmentOffloadRequest {
    String collectionName;
    K2_PAYLOAD_FIELDS(collectionName);
};

// Response to AssignmentOffloadRequest
struct AssignmentOffloadResponse {
    K2_PAYLOAD_EMPTY;
};

}  // namespace dto
}  // namespace k2
