/*
Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2020 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.

Portions copyright (c) 2021 Futurewei Cloud

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

#include <random>
#include <cstdint>
#include <cmath>
#include <cstring>

class RandomContext {
public:
    RandomContext(int seed=0) : _generator(seed) {};

    RandomContext(int seed, std::vector<double> prob) : _generator(seed), _discreteDis(prob.begin(),prob.end()) {};

    // sample a number from [min,max] uniformly at random
    uint32_t UniformRandom(uint32_t min, uint32_t max)
    {
        std::uniform_int_distribution<> dist(min, max);
        return dist(_generator);
    }

    // generate a random string having characters from (char)[33,126] of specified length
    k2::String RandomString(uint32_t length)
    {
        k2::String str(k2::String::initialized_later{}, length);

        for (uint32_t i=0; i<length; ++i) {
            str[i] = (char)UniformRandom(33, 126); // Non-whitespace and non-control ASCII characters
        }

        return str;
    }

    // return a set of unique numbers in [min,max] having num size
    std::set<uint32_t> RandomSetInt(uint32_t num, uint32_t min, uint32_t max){
        // Create a set to hold the random numbers
        std::set<uint32_t> randNos;

        // Generate the random numbers
        while(randNos.size() != num)
        {
            randNos.insert(UniformRandom(min,max));
        }

        return randNos;
    }

    // return an integer i in [0,n) with prob p_i/sum_{0<=j<n}(p_j) according to provided p_js.
    // discreteDis is initialized with the p_js
    uint64_t BiasedInt()
    {
        return _discreteDis(_generator);
    }

private:
    std::mt19937 _generator;
    std::discrete_distribution<> _discreteDis;
};

// Base class for all distributions that generate random items : Uniform, Zipfian, Scrambled Zipfian and Latest
class RandomGenerator {
public:
    virtual uint64_t getValue() = 0; // returns random value according to distribution
    virtual void updateBounds(uint64_t min, uint64_t max) = 0; // returns true if Bounds are successfully updated
    virtual uint64_t getMaxValue() = 0; // returns max value that can be output by distribution
    virtual ~RandomGenerator() = default;
};

class UniformGenerator : public RandomGenerator {
public:
    UniformGenerator(int min, int max, int seed=0) : _generator(seed), _dist(min,max) {};

     uint64_t getValue() override {
         return _dist(_generator);
     }

    void updateBounds(uint64_t min, uint64_t max) override {
         _dist.param(std::uniform_int_distribution<int>::param_type{(int)min, (int)max});
    }

    uint64_t getMaxValue() override {
        return _dist.max();
    }

private:
    std::mt19937 _generator;
    std::uniform_int_distribution<> _dist;
};

class ZipfianGenerator : public RandomGenerator{

public:
    // create zipfian generator for items between min and max (inclusive) with given zipfianconstant
    ZipfianGenerator(uint64_t min, uint64_t max, int seed=0, double zipfianconstant=0.99) : ZipfianGenerator(min,max,zipfianconstant,computeZetan(max-min+1,zipfianconstant),seed) { }

    // create zipfian generator for items between min and max (inclusive) with given zipfianconstant and precomputed parameter zeta
    ZipfianGenerator(uint64_t min, uint64_t max, double zipfianconstant, double zetan, int seed=0) :
                                                _generator(seed), _dist(0,1), _min(min), _items(max - min + 1), _zipfianconstant(zipfianconstant), _zetan(zetan) {
        _zeta2theta = computeZetan(2,_zipfianconstant);
        _alpha = 1.0 / (1.0 - _zipfianconstant);
        _eta = (1 - pow(2.0 / _items, 1 - _zipfianconstant)) / (1 - _zeta2theta / _zetan);
    }

    // sample from the zipfian distribution. The min-th item is the most popular, followed by the min+1th item and so on...
    // use algorithm from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994
    uint64_t getValue() override {
        double u = _dist(_generator);
        double uz = u * _zetan;

        if (uz < 1.0) {
            return _min;
        }

        if (uz < 1.0 + pow(0.5, _zipfianconstant)) {
            return _min + 1;
        }

        uint64_t ret = _min + (uint64_t) ((_items) * pow(_eta * u - _eta + 1, _alpha));
        return ret;
    }

    // compute parameter zeta given n and zipfianconstant
    // compute parameter incrementally when n increases
    static double computeZetan(uint64_t n, double zipfianconstant, double oldZeta=0, uint64_t oldn=0){
        double zetan = oldZeta;

        for(uint64_t i=oldn; i<n; i++){
            zetan += 1.0/(pow(i + 1, zipfianconstant));
        }

        return zetan;
    }

    void updateBounds(uint64_t min, uint64_t max) override { //update all parameters as well
        if(max-min+1==_items){
            _min = min; // only changing min is enough
            return;
        } else if(max-min+1>_items){
            _zetan = computeZetan(max-min+1,_zipfianconstant,_zetan,_items); //update zetan
        } else {
            _zetan = computeZetan(max-min+1,_zipfianconstant); // recompute zetan
        }
        _min = min;
        _items = max-min+1;
        _eta = (1 - pow(2.0 / _items, 1 - _zipfianconstant)) / (1 - _zeta2theta / _zetan);
    }

    uint64_t getMaxValue() override {
        return _min+_items-1;
    }

private:
    std::mt19937 _generator;
    std::uniform_real_distribution<> _dist; // to generate number between 0 and 1
    uint64_t _min; // the min item
    uint64_t _items; // total items
    double _zipfianconstant; // zipfian constant used
    double _zeta2theta, _zetan, _alpha, _eta; //parameters for the distribution
};

// 64 bit FNV hash - used in Scrambled zipfian generator to scatter items in itemspace
uint64_t fnvhash64(uint64_t val) {
    static const int64_t FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
    static const int64_t FNV_PRIME_64 = 1099511628211L;

    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    uint64_t hashval = FNV_OFFSET_BASIS_64;

    for (uint8_t it = 0; it < 8; it++) {
        uint64_t octet = val & 0x00ff;
        val = val >> 8;

        hashval = hashval ^ octet;
        hashval = hashval * FNV_PRIME_64;
    }
    return hashval;
}

// ScrambledZipfianGenerator unlike Zipfian generator ensures that popular items are scattered over the itemspace
class ScrambledZipfianGenerator : public RandomGenerator{
public:
    static constexpr double ZETAN = 26.46902820178302; // precomputed using Zipfian::computeZeta(ITEM_COUNT, ZIPFIAN_CONSTANT)
    static constexpr double ZIPFIAN_CONSTANT = 0.99;
    static const uint64_t ITEM_COUNT = 10000000000L; // item count for the Zipfian distribution used to generate the ScrambledZipfian Generator

    ScrambledZipfianGenerator(uint64_t items, int seed=0) : ScrambledZipfianGenerator(0, items - 1, seed) {} // items - number of items in the distribution

    // Scrambled Zipfian generator for items between min and max
    ScrambledZipfianGenerator(uint64_t min, uint64_t max, int seed=0) : _gen(0, ITEM_COUNT, ZIPFIAN_CONSTANT, ZETAN, seed),
                                                                        _min(min), _itemcount(max - min + 1) {}

    // Sample value from scrambled zipfian distribution - popular items are scattered across the itemspace
    uint64_t getValue() override {
        uint64_t ret = _gen.getValue();
        ret = (_min + fnvhash64(ret)) % _itemcount;
        return ret;
    }

    void updateBounds(uint64_t min, uint64_t max) override {
        _min = min;
        _itemcount = max-min+1;
    }

    uint64_t getMaxValue() override {
        return _min+_itemcount-1;
    }

private:
    ZipfianGenerator _gen;
    uint64_t _min, _itemcount;
};


class LatestGenerator : public RandomGenerator{
public:
    // create latest generator for items between min and max (inclusive) with given zipfianconstant
    LatestGenerator(uint64_t min, uint64_t max, int seed=0) : _gen(min, max, seed), _max(max) {}

    // Sample value from latest distribution - most popular item is the latest inserted item or max
    uint64_t getValue() override {
        uint64_t ret = _gen.getValue();
        return _max - ret;
    }

    void updateBounds(uint64_t min, uint64_t max) override {
        _max = max;
        _gen.updateBounds(min,max);
    }

    uint64_t getMaxValue() override {
        return _max;
    }

private:
    ZipfianGenerator _gen;
    uint64_t _max;
};
