/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

// return a tuple of iterators to the elements before, at, and after the given key.
template <typename KeyT, typename MapT>
auto keyRange(const KeyT& key, MapT& mm) {
    auto end = mm.end();
    // gives iterators (lower, upper)
    // lower is an interator to the first element, not less than key
    // upper is an iterator to the first element greater than key
    auto [at, after] = mm.equal_range(key);

    // to get the element before "key", we need to rewind "at"
    auto before = at != end ? at : after;
    if (before != mm.begin()) {
        // this is only safe to do if we do have some elements present
        --before;
    } else {
        // we can't rewind begin. Set it to end() directly
        before = mm.end();
    }

    // if key exists, "at" and "after" will be different
    auto found = at == after ? end : at;

    return std::tuple(before, found, after);
}

}
