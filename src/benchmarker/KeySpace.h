#pragma once

// std
#include <memory>
#include <random>
#include <string>
// k2b
#include <benchmarker/generator/Generator.h>

namespace k2
{
namespace benchmarker
{

class KeySpace
{
protected:
    std::unique_ptr<Generator> _pGenerator;
    uint64_t _count = 0;
public:
    class iterator
    {
    protected:
        uint64_t _count = 0;
        uint64_t _cursor = 0;
        Generator* _pGenerator;
        Generator::iterator _it;
    public:
        iterator(Generator& generator, uint64_t count, uint64_t cursor)
        : _count(count)
        , _cursor(cursor)
        , _pGenerator(&generator)
        , _it(generator.begin())
        {
            if(cursor == ((uint64_t)-1)) {
                _it = std::move(_pGenerator->end());
            }
        }

        iterator& operator++ ()
        {
            ++_cursor;
            ++_it;

            if(_cursor >= _count) {
                _it = _pGenerator->end();
                _cursor = ((uint64_t)-1);
            }

            return *this;
        }

        bool operator== (const iterator& arg) const
        {
            return (_cursor == arg._cursor && _it == arg._it);
        }

        bool operator!= (const iterator& arg) const
        {
            return !(*this == arg);
        }

        std::string& operator* ()
        {
            return const_cast<std::string&>(*_it);
        }
    };

    KeySpace(std::unique_ptr<Generator> pGenerator)
    : _pGenerator(std::move(pGenerator))
    {
        _count = 0; // unbounded
    }

    KeySpace(std::unique_ptr<Generator> pGenerator, uint64_t count)
    : _pGenerator(std::move(pGenerator))
    , _count(count)
    {
        // empty
    }

    iterator begin()
    {
        return iterator(*_pGenerator.get(), _count, 0);
    }

    iterator end()
    {
        return iterator(*_pGenerator.get(), _count, ((uint64_t)-1)); // make generator = null as the end iterator
    }

};

};
};
