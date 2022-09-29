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

#include "StreamBuf.h"

namespace skv::http {

DFWriteStreamBuf::DFWriteStreamBuf(MPackNodeWriter& mpwriter): _mpwriter(mpwriter), _sz(0) {}

std::streamsize DFWriteStreamBuf::xsputn(const char_type* s, std::streamsize count) {
    std::memcpy(_data + _sz, s, count);
    _sz += count;
    return count;
}

std::streambuf::int_type DFWriteStreamBuf::sputc(char_type ch) {
    _data[_sz] = ch;
    ++_sz;
    return ch;
}

DFWriteStreamBuf::~DFWriteStreamBuf() {
    _mpwriter.write(Binary(_data, _sz, [](){}));
}


DFReadStreamBuf::DFReadStreamBuf(MPackNodeReader& mpreader): _mpreader(mpreader), _index{0} {
    Binary bin;
    _mpreader.read(bin);
    _data = (char *) bin.data();
}
std::streamsize DFReadStreamBuf::xsgetn(char_type* s, std::streamsize count) {
    std::memcpy(s, _data +_index, count);
    _index += count;
    return count;
}
std::streambuf::int_type DFReadStreamBuf::sbumpc() {
    char_type ch = (char_type) _data[_index];
    ++_index;
    return (int_type) ch;
}

}