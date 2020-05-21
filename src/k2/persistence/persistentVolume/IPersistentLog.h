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

namespace k2
{

typedef uint64_t LSN;   //  Log sequence number

//
//  Represent a file containing set of log records
//
class IPersistentLog
{
public:
    virtual LSN getMaxLSN() = 0;
    virtual LSN getMinLSN() = 0;

    struct Record
    {
        LSN lsn;
        Binary data;
    };

    class IIterator
    {
    public:
        virtual bool next();
        virtual Record current();
        virtual bool eof();

        virtual ~IIterator() {}
    };

    std::unique_ptr<IIterator> scan();

    virtual ~IPersistentLog() {}
};

}   //  namespace k2
