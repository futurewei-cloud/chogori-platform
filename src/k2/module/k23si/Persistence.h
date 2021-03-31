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
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/MessageVerbs.h>
#include "Config.h"
#include "Log.h"

namespace k2 {
class Persistence {
public:
    Persistence();

    // flush all pending writes and prevent further writes
    seastar::future<> stop();

    // flush all pending writes to persistence.
    seastar::future<Status> flush();

    // Appends are always asynchronous (buffered locally) until an explicit call to flush()
    // append_cont returns the status of the flush call
    template<typename ValueType>
    seastar::future<Status> append_cont(const ValueType& val) {
        if (_stopped) {
            K2LOG_W(log::skvsvr, "Attempt to append while stopped");
            return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("persistence has stopped"));
        }
        if (!_remoteEndpoint) {
            K2LOG_W(log::skvsvr, "Attempt to append with no configured remote endpoint");
            return seastar::make_ready_future<Status>(dto::K23SIStatus::OperationNotAllowed("persistence is not available"));
        }
        K2LOG_D(log::skvsvr, "appending new write");

        append(val);
        return _chainResponse();
    }

    // Append the given value to the persistence buffer. An explicit call to flush() is needed to send the data out
    template<typename ValueType>
    void append(const ValueType& val) {
        if (_stopped) {
            K2LOG_W(log::skvsvr, "Attempt to append while stopped");
            return;
        }
        if (!_buffer) {
            _buffer = _remoteEndpoint->newPayload();
        }
        _buffer->write(val);
    }

private:
    bool _stopped{false};
    std::unique_ptr<Payload> _buffer;
    std::unique_ptr<TXEndpoint> _remoteEndpoint;
    K23SIConfig _config;
    seastar::future<Status> _flushFut = seastar::make_ready_future<Status>(dto::K23SIStatus::OK);
    seastar::future<Status> _chainResponse();
};

} // ns k2
