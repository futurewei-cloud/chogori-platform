//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/RetryStrategy.h>

namespace k2 {
// implements an example RPC Service which can send/receive messages
class Service : public seastar::weakly_referencable<Service> {
public: // public types
    // The message verbs supported by this service
    enum MsgVerbs: Verb {
        POST = 100,
        GET = 101,
        ACK = 102
    };

public:  // application lifespan
    Service():
        _updateTimer([this]{
            this->sendHeartbeat();
        }),
        _msgCount(0) {
        // Constructors should not do much more than just remembering the passed state because
        // not all dependencies may have been created yet.
        // The initialization should happen in Start() since at that point all deps should have been created
        K2INFO("ctor");
    };

    virtual ~Service() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2INFO("stop");
        _updateTimer.cancel();
        // unregistar all observers
        RPC().registerMessageObserver(MsgVerbs::POST, nullptr);
        RPC().registerMessageObserver(MsgVerbs::GET, nullptr);
        RPC().registerMessageObserver(MsgVerbs::ACK, nullptr);
        RPC().registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

    // called after construction
    void start() {
        // This method is executed on each object which distributed<> created for each core.

        // You can store the endpoint for more efficient communication
        _heartbeatTXEndpoint = RPC().getTXEndpoint("tcp+k2rpc://127.0.0.1:14000");
        if (!_heartbeatTXEndpoint) {
            throw std::runtime_error("unable to get an endpoint for url");
        }

        K2INFO("Registering message handlers");

        RPC().registerMessageObserver(MsgVerbs::POST,
            [this](Request&& request) mutable {
                this->handlePOST(std::move(request));
            });

        RPC().registerMessageObserver(MsgVerbs::GET,
            [this](Request&& request) mutable {
                this->handleGET(std::move(request));
            });

        RPC().registerMessageObserver(MsgVerbs::ACK,
            [this](Request&& request) mutable {
                auto received = getPayloadString(request.payload.get());
                K2INFO("Received ACK from " << request.endpoint.getURL() <<
                      ", and payload: " << received);
            });

        RPC().registerLowTransportMemoryObserver([](const String& ttype, size_t requiredReleaseBytes) {
            // we should release any payloads we're holding
            // ideally, we should release enough payloads whose size() sums up to more than requiredReleaseBytes
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        // also start the heartbeat timer
        _updateTimer.arm(_updateTimerInterval);
        _stopped = false;
    }

public: // Work generators
    void sendHeartbeat() {
        K2INFO("Sending reqid="<< _msgCount);
        // send a GET
        {
            String msg("Requesting GET reqid=");
            msg += std::to_string(_msgCount++);

            std::unique_ptr<Payload> request = _heartbeatTXEndpoint->newPayload();
            request->getWriter().write(msg.c_str(), msg.size()+1);
            // straight Send sends requests without any form of retry. Underlying transport may or may not
            // attempt redelivery (e.g. TCP packet reliability)
            RPC().send(GET, std::move(request), *_heartbeatTXEndpoint);
        }

        // send a POST where we expect to receive a reply
        {
            _msgCount++;
            auto msground = _msgCount;

            // retry at 10ms, 50ms(=10ms*5), and 250ms(=50ms*5)
            auto retryStrategy = seastar::make_lw_shared<ExponentialBackoffStrategy>();
            retryStrategy->withRetries(3).withStartTimeout(10ms).withRate(5);

            // NB: since seastar future continuations may be scheduled to run at later points,
            // it may be possible that the Service instance goes away in a middle of a retry.
            // To avoid a segmentation fault, either use copies, or as in this example - weak reference
            (void) retryStrategy->run([self=weak_from_this(), msground](size_t retriesLeft, Duration timeout) {
                K2INFO("Sending with retriesLeft=" << retriesLeft << ", and timeout="
                       << timeout.count() << ", in reqid="<< msground);
                if (!self) {
                    K2INFO("Stopping retry since dispatcher has exited");
                    return seastar::make_ready_future<>();
                }
                String msgData = "Requesting POST reqid=";
                msgData += std::to_string(msground);
                // In this example, we must create a new payload each time we want to retry.
                // The reason is that once we attempt a send over a transport, we move ownership of payload
                // to the transport and may not be able to get the original packets back.
                // e.g. one we place the packets into the DPDK mem queue, we might not be able to obtain the
                // exact same packets back unless we do some cooperative refcounting with the dpdk internals
                std::unique_ptr<Payload> msg = self->_heartbeatTXEndpoint->newPayload();
                msg->getWriter().write(msgData.c_str(), msgData.size()+1);

                // send a request with expected reply. Since we expect a reply, we must specify a timeout
                return RPC().sendRequest(POST, std::move(msg), *self->_heartbeatTXEndpoint, timeout)
                .then([msground](std::unique_ptr<Payload> payload) {
                    // happy case is chained right onto the dispatcher Send call
                    auto received = getPayloadString(payload.get());
                    K2INFO("Received reply for reqid=" << msground << " : " << received);
                    // if the application wants to retry again (in case of some retryable server error)
                    // ServiceMessage msg(payload);
                    // if (msg.Status != msg.StatusOK) {
                    //    return make_exception_future<>(std::exception(msg.Status));
                    // }
                });

            }) // Do returns an empty future here for the response either successful if any of the tries succeeded, or an exception.
            .handle_exception([msground](auto exc){
                // here we handle the exception case (e.g. timeout, unable to connect, invalid endpoint, etc)
                // this is the handler which handles the exception AFTER the retry strategy is exhausted
                K2ERROR("Failed to get response for message reqid=" << msground << " : " << exc);
            }).finally([self=weak_from_this(), retryStrategy, msground](){
                // to keep the retry strategy around while we're working on it, use a shared ptr and capture it
                // here by copy so that it is only released after Do completes.
                K2DEBUG("done with retry strategy for reqid=" << msground);
                if (self) {
                    // reschedule again since we're still alive
                    self->_updateTimer.arm(_updateTimerInterval);
                }
            });
        }
    }

public:
    // Message handlers
    void handlePOST(Request&& request) {
        auto received = getPayloadString(request.payload.get());
        K2INFO("Received POST message from endpoint: " << request.endpoint.getURL()
              << ", with payload: " << received);
        String msgData("POST Message received reqid=");
        msgData += std::to_string(_msgCount++);

        std::unique_ptr<Payload> msg = request.endpoint.newPayload();
        msg->getWriter().write(msgData.c_str(), msgData.size()+1);
        // respond to the client's request
        RPC().sendReply(std::move(msg), request);
    }

    void handleGET(Request&& request) {
        auto received = getPayloadString(request.payload.get());
        K2INFO("Received GET message from endpoint: " << request.endpoint.getURL()
              << ", with payload: " << received);
        String msgData("GET Message received reqid=");
        msgData += std::to_string(_msgCount++);

        std::unique_ptr<Payload> msg = request.endpoint.newPayload();
        msg->getWriter().write(msgData.c_str(), msgData.size()+1);

        // Here we just forward the message using a straight Send and we don't expect any responses to our forward
        RPC().send(ACK, std::move(msg), request.endpoint);
    }

private:
    static std::string getPayloadString(Payload* payload) {
        if (!payload) {
            return "NO_PAYLOAD_RECEIVED";
        }
        std::string result;
        for (auto& binary: payload->release()) {
            K2DEBUG("Processing received binary of size=" << binary.size());
            auto datap = binary.get();
            for (size_t i = 0; i < binary.size(); ++i) {
                if (std::isprint(static_cast<unsigned char>(datap[i]))) {
                    result.append(1, datap[i]);
                }
                else {
                    result.append(1, '.');
                }
            }
        }
        return result;
    }

    seastar::timer<> _updateTimer;
    static constexpr auto _updateTimerInterval = 1s;
    std::unique_ptr<TXEndpoint> _heartbeatTXEndpoint;
    // send around our message count
    uint64_t _msgCount;

    // flag we need to tell if we've been stopped
    bool _stopped;

}; // class Service
}// namespace k2

int main(int argc, char** argv) {
    k2::App app;
    app.addApplet<k2::Service>();
    return app.start(argc, argv);
}
