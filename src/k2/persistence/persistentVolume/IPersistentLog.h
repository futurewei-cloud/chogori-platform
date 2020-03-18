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
