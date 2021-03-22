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
    K2LOG_D(log::skvsvr, "Stopping with {} pending promises", _pendingProms.size());

    return flush().discard_result();
}

seastar::future<Status> Persistence::flush() {
    K2LOG_D(log::skvsvr, "flush with bs={} and {} pending promises", (_buffer? _buffer->getSize() : 0), _pendingProms.size());
    if (!_buffer) {
        K2ASSERT(log::skvsvr, _pendingProms.size() == 0, "There are {} pending proms", _pendingProms.size());
        return seastar::make_ready_future<Status>(Statuses::S200_OK(""));
    }

    // In this method we have to execute a bunch of async steps:
    // 1. persist the current batch (1 op)
    // 2. notify the writers that their writes completed (N concurrent ops)
    // As 1 and 2 above are happening, there could be new modifications arriving (e.g. new WIs being
    // placed, or even writes issued by the continuations on the promises satisfied by 2).
    // To prevent all of these cases of concurrent modification of the buffers and promises,
    // we swap those out here, forcing any new modifications to be appended to a new batch.

    // swap out the pending promises, clearing the active batch promises
    std::vector<seastar::promise<>> proms;
    proms.swap(_pendingProms);

    // move the buffered data into a single request and delete the buffer.
    // Any writes after this point will be appended to a new buffer/batch
    dto::K23SI_PersistenceRequest<Payload> request{};
    request.value.val = std::move(*_buffer);
    _buffer.reset(nullptr);

    return RPC().callRPC<dto::K23SI_PersistenceRequest<Payload>, dto::K23SI_PersistenceResponse>
        (dto::Verbs::K23SI_Persist, request, *_remoteEndpoint, _config.persistenceTimeout())
        .then_wrapped([proms=std::move(proms)] (auto&& fut) mutable {
        K2LOG_D(log::skvsvr, "flushed. Notifying {} pending promises", proms.size());

            if (fut.failed()) {
                auto exc = fut.get_exception();
                for (auto&prom: proms) prom.set_exception(exc);
                return seastar::make_exception_future<Status>(exc);
            }

            for (auto&prom: proms) prom.set_value();
            return seastar::make_ready_future<Status>(std::get<0>(fut.get()));
        });
}
}
