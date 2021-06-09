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
namespace dto {


struct PartitionMetdataRecord{
    String plogId;
    uint32_t sealed_offset;
    K2_PAYLOAD_FIELDS(plogId, sealed_offset);
    K2_DEF_FMT(PartitionMetdataRecord, plogId, sealed_offset);
};

// Request to create a Metadata Log Stream Record 

struct MetadataPutRequest {
    String partitionName;
    uint32_t sealed_offset;
    String new_plogId;
    K2_PAYLOAD_FIELDS(partitionName, sealed_offset, new_plogId);
    K2_DEF_FMT(MetadataPutRequest, partitionName, sealed_offset, new_plogId);
};

struct MetadataPutResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(MetadataPutResponse);
};

struct MetadataGetRequest {
    String partitionName;
    K2_PAYLOAD_FIELDS(partitionName);
    K2_DEF_FMT(MetadataGetRequest, partitionName);
};

struct MetadataGetResponse {
    std::vector<PartitionMetdataRecord> records;
    K2_PAYLOAD_FIELDS(records);
    K2_DEF_FMT(MetadataGetResponse, records);
};

struct LogStreamReadContinuationToken{
    String plogId;
    uint32_t offset;
    K2_PAYLOAD_FIELDS(plogId, offset);
    K2_DEF_FMT(LogStreamReadContinuationToken, plogId, offset);
};

struct AppendRequest{
    Payload payload;
    K2_PAYLOAD_FIELDS(payload);
    K2_DEF_FMT(AppendRequest);
};

struct AppendWithIdAndOffsetRequest{
    Payload payload;
    String plogId;
    uint32_t offset;
    K2_PAYLOAD_FIELDS(payload, plogId, offset);
    K2_DEF_FMT(AppendWithIdAndOffsetRequest, plogId, offset);
};

struct AppendResponse{
    String plogId;
    uint32_t current_offset;
    K2_PAYLOAD_FIELDS(plogId, current_offset);
    K2_DEF_FMT(AppendResponse, plogId, current_offset);
};

struct ReadRequest{
    String start_plogId;
    uint32_t start_offset;
    uint32_t size;
    K2_PAYLOAD_FIELDS(start_plogId, start_offset, size);
    K2_DEF_FMT(ReadRequest, start_plogId, start_offset, size);
};

struct ReadWithTokenRequest{
    LogStreamReadContinuationToken token;
    uint32_t size;
    K2_PAYLOAD_FIELDS(token, size);
    K2_DEF_FMT(ReadWithTokenRequest, token, size);
};

struct ReadResponse{
    LogStreamReadContinuationToken token;
    Payload payload;
    K2_PAYLOAD_FIELDS(token, payload);
    K2_DEF_FMT(ReadResponse, token);
};

}  // namespace dto
}  // namespace k2
