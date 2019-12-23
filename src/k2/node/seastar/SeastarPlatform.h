#pragma once

#include <k2/common/ModuleId.h>
#include <k2/node/Module.h>
#include <k2/node/ISchedulingPlatform.h>
#include <k2/node/Node.h>
#include <memory>

#include <seastar/net/api.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>


using namespace seastar;
using namespace seastar::net;

namespace k2
{

class SeastarPlatform : public ISchedulingPlatform
{
protected:
    class TCPServer
    {
    protected:
        seastar::lw_shared_ptr<seastar::server_socket> _listener;
        Node& _node;

        class Connection
        {
        public:
            typedef char CharType;
            seastar::connected_socket _socket;
            seastar::socket_address _addr;
            seastar::input_stream<CharType> _in;
            seastar::output_stream<CharType> _out;
            Binary _headerBuffer;
            seastar::promise<> _endRoundPromise;
            PartitionMessage::Header& getHeader() { return *(PartitionMessage::Header*)_headerBuffer.get_write(); }

            seastar::future<> finishRound() //  Current message exchange round is done
            {
                _headerBuffer = Binary();
                _endRoundPromise = seastar::promise<>();
                return _out.flush();
            }

            String getClientAddressString()
            {
                return "Client";    //  TODO: use _addr to format this
            }

            Connection(seastar::connected_socket&& socket, seastar::socket_address addr)
                : _socket(std::move(socket))
                , _addr(addr)
                , _in(_socket.input())
                , _out(_socket.output())
            { }
        };

        class MessageExchangeRound : public IClientConnection
        {
        public:
            seastar::lw_shared_ptr<Connection> _connection;
            bool _responded = false;
            Payload _outPayload;
            ResponseMessage::Header* _header;

            MessageExchangeRound(seastar::lw_shared_ptr<Connection> connection) : _connection(connection)
            {
                PayloadWriter writer = _outPayload.getWriter();
                auto rcode = writer.reserveContiguousStructure(_header); //  Always must have space for a response header
                assert(rcode);
            }

            PayloadWriter getResponseWriter() override
            {
                assert(!_responded);
                return _outPayload.getWriter(sizeof(ResponseMessage::Header));
            }

            void sendResponse(Status status, uint32_t code) override
            {
                assert(!_responded);

                _header->status = status;
                _header->moduleCode = code;
                _header->messageSize = _outPayload.getSize()-sizeof(ResponseMessage::Header);
                _responded = true;

                _connection->_out.write(Payload::toPacket(std::move(_outPayload)))
                    .then([con = _connection] { con->_endRoundPromise.set_value(); });
            }
        };

        seastar::future<> processMessage(lw_shared_ptr<Connection> connection)
        {
            return connection->_in.read_exactly(sizeof(PartitionMessage::Header))
                .then([this, connection] (Binary&& headerBuffer)
                {
                    connection->_headerBuffer = std::move(headerBuffer);
                    return connection->_in.read_exactly(connection->getHeader().messageSize);
                })
                .then([this, connection] (Binary&& data)
                {
                    std::vector<Binary> buffers;
                    buffers.push_back(std::move(data));

                    PartitionRequest request;
                    request.message = std::make_unique<PartitionMessage>(
                        connection->getHeader().messageType,
                        connection->getHeader().partition,
                        std::move(connection->getClientAddressString()),
                        Payload(std::move(buffers), connection->getHeader().messageSize));
                    request.client = std::make_unique<MessageExchangeRound>(connection);
                    seastar::future<> endRoundFuture = connection->_endRoundPromise.get_future();

                    _node.assignmentManager.processMessage(request);

                    return std::move(endRoundFuture);
                });
        }

        void startTransport()
        {
            NodeEndpointConfig nodeConfig = _node.getEndpoint();
            assert(nodeConfig.type == NodeEndpointConfig::IPv4);

            seastar::listen_options lo;
            lo.reuse_address = true;
            _listener = seastar::engine().listen(make_ipv4_address(nodeConfig.ipv4.address, nodeConfig.ipv4.port), lo);

            K2INFO("Start transport for shard #" << engine().cpu_id() << " on endpoint {ipv4:"
                << (((nodeConfig.ipv4.address)>>24)&0xFF) << "." << (((nodeConfig.ipv4.address)>>16)&0xFF)
                << "." << (((nodeConfig.ipv4.address)>>8)&0xFF) << "." << ((nodeConfig.ipv4.address)&0xFF)
                << " port:" << nodeConfig.ipv4.port << "}");

            seastar::keep_doing([this]
            {
                return _listener->accept()
                    .then([this] (connected_socket fd, socket_address addr) mutable
                    {
                        seastar::lw_shared_ptr<Connection> connection = seastar::make_lw_shared<Connection>(std::move(fd), addr);
                        processMessage(connection).then([connection] { return connection->finishRound(); })
                        //
                        //  Repeated read currently somehow end up with returning garbage, even when client close the connection
                        //  immediately after response. I'll let transport team to figure it out later, may be some flush is required.
                        //
                        //seastar::do_until(
                        //    [connection] { return connection->_in.eof(); }, //  Until condition
                        //    [connection, this]
                        //    {
                        //        return processMessage(connection).then([connection] { return connection->finishRound(); });
                        //    }
                        //)
                        .finally([connection] { return connection->_out.close().finally([connection]{}); });
                    })
                    .handle_exception([] (std::exception_ptr ep)
                    {
                        try
                        {
                            std::rethrow_exception(ep);
                        } catch(const std::exception& e)
                        {
                            K2ERROR("Caught exception " << e.what());
                            throw e;
                        }
                    });
            }).or_terminate();
        }

        void startTaskProcessor()
        {
            seastar::keep_doing([this]
            {
                _node.processTasks();
                return seastar::make_ready_future<>();
            }).or_terminate();
        }

    public:
        TCPServer(INodePool& nodePool) : _node(nodePool.getCurrentNode()) { }

        void start()
        {
            startTransport();
            startTaskProcessor();
        }

        future<> stop() { return make_ready_future<>(); }
    };

public:
    SeastarPlatform() {}

    Status run(INodePool& pool)
    {
        seastar::distributed<TCPServer> server;
        seastar::app_template app;

        // TODO: So far couldn't find a good way to configure Seastar thorugh app. add_options without using command arguments.
        // Will do more research later and fix it. Now just configure through argument
        std::string nodesCountArg = std::string("-c") + std::to_string(pool.getNodesCount());

        const char* argv[] = { "NodePool", nodesCountArg.c_str(), nullptr };
        int argc = sizeof(argv) / sizeof(*argv) - 1;

        int ret = app.run_deprecated(
            argc,   // argc
            (char**)argv,                   // argv
            [&]
            {
                engine().at_exit([&] { return server.stop(); });
                return server.start(std::ref(pool))
                    .then([&server] { return server.invoke_on_all(&TCPServer::start); });
            });

        return ret == 0 ? Status::Ok : Status::SchedulerPlatformStartingFailure;
    }

    uint64_t getCurrentNodeId() override
    {
        return engine().cpu_id();
    }

    void delay(Duration delay, std::function<void()>&& callback) override
    {
        seastar::sleep(delay).then([cb = std::move(callback)] { cb(); });
    }
};

} //  namespace k2
