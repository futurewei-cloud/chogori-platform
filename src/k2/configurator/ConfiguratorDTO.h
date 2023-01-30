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
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>

namespace k2 {

struct DELETE_ConfiguratorRequest {
    String key;
    String value;
    bool applyToAll;
    K2_PAYLOAD_FIELDS(key, value, applyToAll);
    K2_DEF_FMT(DELETE_ConfiguratorRequest, key, value, applyToAll);
};

struct DELETE_ConfiguratorResponse {
    String key;
    K2_PAYLOAD_FIELDS(key);
    K2_DEF_FMT(DELETE_ConfiguratorResponse, key);
};

struct SET_ConfiguratorRequest {
    String key;
    String value;
    bool applyToAll;
    K2_PAYLOAD_FIELDS(key, value, applyToAll);
    K2_DEF_FMT(SET_ConfiguratorRequest, key, value, applyToAll);
};

struct SET_ConfiguratorResponse {
    String key;
    K2_PAYLOAD_FIELDS(key);
    K2_DEF_FMT(SET_ConfiguratorResponse, key);
};

struct GET_ConfiguratorRequest {
    String key;
    K2_PAYLOAD_FIELDS(key);
    K2_DEF_FMT(GET_ConfiguratorRequest, key);
};

struct GET_ConfiguratorResponse {
    String key;
    String value;
    K2_PAYLOAD_FIELDS(key, value);
    K2_DEF_FMT(GET_ConfiguratorResponse, key, value);
};

} // ns k2
