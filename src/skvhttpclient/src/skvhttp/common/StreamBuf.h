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
#include <skvhttp/common/Binary.h>
#include <skvhttp/common/Common.h>
#include <skvhttp/common/Serialization.h>
#include <skvhttp/mpack/MPackSerialization.h>

namespace skv::http {

const size_t MAX_SERIALIZED_SIZE_CPP_DEC_FLOATS = 100;

class MPackNodeWriter;
class MPackNodeReader;

class DFWriteStreamBuf : public std::streambuf {
public:
    DFWriteStreamBuf(MPackNodeWriter& mpwriter);
    std::streamsize xsputn(const char_type* s, std::streamsize count);
    int_type sputc(char_type ch);
    ~DFWriteStreamBuf();

    MPackNodeWriter& _mpwriter;
    char _data[MAX_SERIALIZED_SIZE_CPP_DEC_FLOATS];
    size_t _sz;
};

class DFReadStreamBuf : public std::streambuf {
public:
    DFReadStreamBuf(MPackNodeReader& mpreader);
    std::streamsize xsgetn(char_type* s, std::streamsize count);
    int_type sbumpc();

    MPackNodeReader& _mpreader;
    char *_data;
    size_t _index;
};

}