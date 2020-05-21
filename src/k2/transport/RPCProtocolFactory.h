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
// stl
#include <functional> // functions

// third-party
#include <seastar/core/shared_ptr.hh> // seastar's pointers
#include <seastar/core/distributed.hh> // seastar's distributed/sharded stuff

// k2
#include "IRPCProtocol.h"

namespace k2 {

// This is a helper class used to construct concrete protocols of a given type on demand in the process
// of building a distributed<> object.
// This class should be used as a distributed<> container
class RPCProtocolFactory {

public: // types
    // Distributed version of the class
    typedef seastar::distributed<RPCProtocolFactory> Dist_t;

    // Factory signature for building protocols
    typedef std::function<seastar::shared_ptr<IRPCProtocol>()> BuilderFunc_t;

public: // lifecycle
    // Make a new RPCProtocolFactory with the given IRPCProtocol builder. This builder should produce
    // instances of concrete protocols (e.g. TCPRPCProtocol)
    RPCProtocolFactory(BuilderFunc_t builder);

    // dtor
    ~RPCProtocolFactory();

public: // interface
    // Obtain the IRPCProtocol instance
    seastar::shared_ptr<IRPCProtocol> instance();

public: // distributed<> interface
    // called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    seastar::future<> stop();

    // Should be called by user when all distributed objects have been created
    void start();

private:
    // the factory for building a protocol
    BuilderFunc_t _builder;

    // this is where we keep the instance returned by the factory
    seastar::shared_ptr<IRPCProtocol> _instance;
}; // RPCProtocolFactory

} // k2
