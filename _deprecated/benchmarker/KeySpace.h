#pragma once

// std
#include <memory>
#include <random>
#include <string>
// k2b
#include <k2/benchmarker/generator/Generator.h>

namespace k2
{
namespace benchmarker
{

class KeySpace
{
protected:
    std::unique_ptr<Generator> _pGenerator;
    const uint64_t _count;
public:
    class iterator
    {
    friend class KeySpace;
    protected:
        uint64_t _count = 0;
        uint64_t _cursor = 0;
        Generator* _pGenerator;
        Generator::iterator _it;

        iterator(Generator& generator, uint64_t count)
        : _count(count)
        , _cursor(0)
        , _pGenerator(&generator)
        , _it(_pGenerator->begin())
        {
            // empty
        }

        iterator()
        : _count(0)
        , _cursor(0)
        , _pGenerator(nullptr)
        {
            // empty
        }

    public:
        iterator& operator++ ()
        {
            ++_cursor;
            ++_it;

            if(_count!=0 && _cursor >= _count) {
                _it = _pGenerator->end();
                _pGenerator = nullptr;
            }

            return *this;
        }

        bool operator== (const iterator& arg) const
        {
            return (_pGenerator==nullptr && arg._pGenerator==nullptr) || (_cursor == arg._cursor && _it == arg._it);
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
    , _count(0) // unbounded
    {
        // empty
    }

    KeySpace(std::unique_ptr<Generator> pGenerator, uint64_t count)
    : _pGenerator(std::move(pGenerator))
    , _count(count)
    {
        // empty
    }

    iterator begin()
    {
        return iterator(*_pGenerator.get(), _count);
    }

    iterator end()
    {
        return iterator();
    }

};

};
};
