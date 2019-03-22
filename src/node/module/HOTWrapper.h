#pragma once

#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

using namespace std;

namespace k2 {
    using TrieType = hot::singlethreaded::HOTSingleThreaded<const string*, idx::contenthelpers::StringKeyExtractor>;

}