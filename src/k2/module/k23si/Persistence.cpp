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

#include "Persistence.h"

namespace k2 {

Persistence::Persistence() {
    int id = seastar::this_shard_id();
    String endpoint = _config.persistenceEndpoint()[id % _config.persistenceEndpoint().size()];
    _remoteEndpoint = RPC().getTXEndpoint(endpoint);
    K2LOG_I(log::skvsvr, "ctor with endpoint: {}", _remoteEndpoint->url);
}

seastar::future<> Persistence::stop() {
    _stopped = true;
    K2LOG_D(log::skvsvr, "Stopping");

    return flush().then([this] (auto&&) mutable { return std::move(_flushFut);}).discard_result();
}

seastar::future<Status> Persistence::flush() {
    K2LOG_D(log::skvsvr, "flush with bs={}", (_buffer? _buffer->getSize() : 0));
    seastar::promise<Status> prom;
    auto resp = prom.get_future();
    if (!_buffer) {
        return _chainResponse();
    }

    // move the buffered data into a single request and delete the buffer.
    // Any writes after this point will be appended to a new buffer/batch
    dto::K23SI_PersistenceRequest<Payload> request{};
    request.value.val = std::move(*_buffer);
    _buffer.reset(nullptr);

    _flushFut = _flushFut
       .then([this, request=std::move(request)] (auto&& status) mutable {
            if (!status.is2xxOK()) {
                // previous flush did not succeed. just pass this along
                return RPCResponse(std::move(status), dto::K23SI_PersistenceResponse{});
            }
            return RPC().callRPC<dto::K23SI_PersistenceRequest<Payload>, dto::K23SI_PersistenceResponse>
            (dto::Verbs::K23SI_Persist, request, *_remoteEndpoint, _config.persistenceTimeout());
       })
       .then([] (auto&& result) mutable {
            auto& [status, _] = result;
            // extract the RPC status and send it down the chain
            K2LOG_D(log::skvsvr, "flushed with status: {}", status);

            return seastar::make_ready_future<Status>(std::move(status));
        });

    return _chainResponse();
}

seastar::future<Status> Persistence::_chainResponse() {
    seastar::promise<Status> prom;
    auto fut = prom.get_future();
    _flushFut = _flushFut.then([prom=std::move(prom)] (auto&& status) mutable {
        // set status for the captured promise as well as chained futures
        prom.set_value(status);
        return seastar::make_ready_future<Status>(std::move(status));
    });
    return fut;
}

} // ns k2
