#pragma once

// std
#include <random>
#include <string>

namespace k2
{
namespace benchmarker
{

class Generator
{
friend class iterator;

public:
    class iterator
    {
    protected:
        Generator* _pGenerator;
        std::string _cursor;
    public:
        iterator(Generator& generator, const std::string& cursor)
        : _pGenerator(&generator)
        , _cursor(cursor)
        {
            // empty
        }

        virtual bool operator== (const iterator& arg) const
        {
            return (arg._cursor == _cursor);
        }

        virtual bool operator!= (const iterator& arg) const
        {
            return !(*this==arg);
        }

        virtual std::string& operator* () const
        {
            return const_cast<std::string&>(_cursor);
        }

        virtual iterator& operator++ ()
        {
            _cursor = _pGenerator->next();

            return *this;
        }
    };

    virtual iterator begin()
    {
        return iterator(*this, next());
    }

    virtual iterator end()
    {
        return iterator(*this, std::string());
    }

protected:
    virtual std::string next()
    {
        return std::string();
    }
};

};
};
