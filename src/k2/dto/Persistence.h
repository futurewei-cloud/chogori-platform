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
// This file contains DTOs for K2 Plog Service

namespace k2 {
namespace dto {

struct PlogCreateRequest {
    String plogId;
    K2_PAYLOAD_FIELDS(plogId);
    K2_DEF_FMT(PlogCreateRequest, plogId);
};

struct PlogCreateResponse {
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(PlogCreateResponse);
};

struct PlogAppendRequest {
    String plogId;
    uint32_t offset;
    Payload payload;
    K2_PAYLOAD_FIELDS(plogId, offset, payload);
    K2_DEF_FMT(PlogAppendRequest, plogId, offset);
};

struct PlogAppendResponse {
    uint32_t newOffset;
    Payload return_payload;
    K2_PAYLOAD_FIELDS(newOffset, return_payload);
    K2_DEF_FMT(PlogAppendResponse, newOffset);
};

struct PlogReadRequest {
    String plogId;
    uint32_t offset;
    uint32_t size;
    K2_PAYLOAD_FIELDS(plogId, offset, size);
    K2_DEF_FMT(PlogReadRequest, plogId, offset, size);
};

struct PlogReadResponse {
    Payload payload;
    K2_PAYLOAD_FIELDS(payload);
    K2_DEF_FMT(PlogReadResponse);
};

struct PlogSealRequest {
    String plogId;
    uint32_t truncateOffset;
    K2_PAYLOAD_FIELDS(plogId, truncateOffset);
    K2_DEF_FMT(PlogSealRequest, plogId, truncateOffset);
};

struct PlogSealResponse {
    uint32_t sealedOffset;
    K2_PAYLOAD_FIELDS(sealedOffset);
    K2_DEF_FMT(PlogSealResponse, sealedOffset);
};

struct PlogGetStatusRequest {
    String plogId;
    K2_PAYLOAD_FIELDS(plogId);
    K2_DEF_FMT(PlogGetStatusRequest, plogId);
};

struct PlogGetStatusResponse {
    uint32_t currentOffset;
    bool sealed;
    K2_PAYLOAD_FIELDS(currentOffset, sealed);
    K2_DEF_FMT(PlogGetStatusResponse, currentOffset, sealed);
};

struct PlogCreateError : public std::exception {
    String what_str;
    PlogCreateError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct PlogStatusError : public std::exception {
    String what_str;
    PlogStatusError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct PlogReadError : public std::exception {
    String what_str;
    PlogReadError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct PlogAppendError : public std::exception {
    String what_str;
    PlogAppendError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct PlogSealError : public std::exception {
    String what_str;
    PlogSealError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct PlogGetStatusError : public std::exception {
    String what_str;
    PlogGetStatusError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};


}  // namespace dto
}  // namespace k2
