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

struct ContinuationToken{
    String plogId;
    uint32_t offset;
    K2_PAYLOAD_FIELDS(plogId, offset);
    K2_DEF_FMT(ContinuationToken, plogId, offset);
};


struct LogStreamBaseExistError : public std::exception {
    String what_str;
    LogStreamBaseExistError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamBasePersistError : public std::exception {
    String what_str;
    LogStreamBasePersistError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamBaseRedundantPlogError : public std::exception {
    String what_str;
    LogStreamBaseRedundantPlogError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamBaseReadError : public std::exception {
    String what_str;
    LogStreamBaseReadError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamRetrieveError : public std::exception {
    String what_str;
    LogStreamRetrieveError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct MetadataPersistError : public std::exception {
    String what_str;
    MetadataPersistError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct MetadataGetError : public std::exception {
    String what_str;
    MetadataGetError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamBaseReload : public std::exception {
    String what_str;
    LogStreamBaseReload(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

}  // namespace dto
}  // namespace k2
