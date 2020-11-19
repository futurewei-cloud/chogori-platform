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

// Request to create a Metadata Log Stream Record 
struct MetadataLogStreamRegisterRequest {
    String name;
    String plogId;
    K2_PAYLOAD_FIELDS(name, plogId);
};

struct MetadataLogStreamRegisterResponse {
    K2_PAYLOAD_EMPTY;
};

struct MetadataLogStreamUpdateRequest {
    String name;
    uint32_t sealedOffset;
    String newPlogId;
    K2_PAYLOAD_FIELDS(name, sealedOffset, newPlogId);
};

struct MetadataLogStreamUpdateResponse {
    K2_PAYLOAD_EMPTY;
};

struct MetadataLogStreamGetRequest {
    String name;
    K2_PAYLOAD_FIELDS(name);
};

struct MetadataElement{
    String name;
    uint32_t offset;
    K2_PAYLOAD_FIELDS(name, offset);
};

struct MetadataLogStreamGetResponse {
    std::vector<MetadataElement> streamLog;
    K2_PAYLOAD_FIELDS(streamLog);
};

struct LogStreamExistError : public std::exception {
    String what_str;
    LogStreamExistError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamMetadataRegisterError : public std::exception {
    String what_str;
    LogStreamMetadataRegisterError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamMetadataObtainError : public std::exception {
    String what_str;
    LogStreamMetadataObtainError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamMetadataUpdateError : public std::exception {
    String what_str;
    LogStreamMetadataUpdateError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};

struct LogStreamBackupPlogError : public std::exception {
    String what_str;
    LogStreamBackupPlogError(String s="") : what_str(std::move(s)) {}
    virtual const char* what() const noexcept override { return what_str.c_str(); }
};


}  // namespace dto
}  // namespace k2
