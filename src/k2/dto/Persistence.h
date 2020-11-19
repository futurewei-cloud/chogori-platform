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
};

struct PlogCreateResponse {
    K2_PAYLOAD_EMPTY;
};

struct PlogAppendRequest {
    String plogId;
    uint32_t offset;
    Payload payload;
    K2_PAYLOAD_FIELDS(plogId, offset, payload);
};

struct PlogAppendResponse {
    uint32_t newOffset;
    K2_PAYLOAD_FIELDS(newOffset);
};

struct PlogReadRequest {
    String plogId;
    uint32_t offset;
    uint32_t size;
    K2_PAYLOAD_FIELDS(plogId, offset, size);
};

struct PlogReadResponse {
    Payload payload;
    K2_PAYLOAD_FIELDS(payload);
};

struct PlogSealRequest {
    String plogId;
    uint32_t truncateOffset;
    K2_PAYLOAD_FIELDS(plogId, truncateOffset);
};

struct PlogSealResponse {
    uint32_t sealedOffset;
    K2_PAYLOAD_FIELDS(sealedOffset);
};

struct PlogInfoRequest {
    String plogId;
    K2_PAYLOAD_FIELDS(plogId);
};

struct PlogInfoResponse {
    uint32_t currentOffset;
    bool sealed;
    K2_PAYLOAD_FIELDS(currentOffset, sealed);
};

struct PlogCreateError : public std::exception {
    String what_str;
    PlogCreateError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct PlogInfoError : public std::exception {
    String what_str;
    PlogInfoError(String s="") : what_str(std::move(s)) {}
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


}  // namespace dto
}  // namespace k2
