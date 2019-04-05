#pragma once

#include <common/ModuleId.h>
#include <node/Module.h>
#include <memory>
#include <seastar/core/distributed.hh>
#include <seastar/net/api.hh>
#include <seastar/core/future.hh>
#include <seastar/core/app-template.hh>
#include <node/AssignmentManager.h>


using namespace seastar;
using namespace seastar::net;

#define TRACE() { std::cout << __FUNCTION__ << ":" << __LINE__ << std::endl << std::flush; }

namespace k2
{

class SeastarPlatform
{
protected:
    class TCPServer
    {
    protected:
        seastar::lw_shared_ptr<seastar::server_socket> _listener;
        AssignmentManager _assignmentManager;

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
                assert(writer.getContiguousStructure(_header)); //  Always must have space for a response header
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
                .then([this, connection] (temporary_buffer<char>&& headerBuffer)
                {
                    connection->_headerBuffer = moveBinary(headerBuffer);
                    return connection->_in.read_exactly(connection->getHeader().messageSize);
                })
                .then([this, connection] (temporary_buffer<char>&& data)
                {
                    std::vector<Binary> buffers;
                    buffers.push_back(moveBinary(data));

                    PartitionRequest request;
                    request.message = std::make_unique<PartitionMessage>(
                        connection->getHeader().messageType,
                        connection->getHeader().partition,
                        std::move(connection->getClientAddressString()),
                        Payload(std::move(buffers), connection->getHeader().messageSize));
                    request.client = std::make_unique<MessageExchangeRound>(connection);
                    seastar::future<> endRoundFuture = connection->_endRoundPromise.get_future();

                    _assignmentManager.processMessage(request);

                    return std::move(endRoundFuture);
                });
        }

        void startTransport()
        {
            NodeEndpointConfig nodeConfig = _assignmentManager.getNodePool().getEndpoint(engine().cpu_id());
            assert(nodeConfig.type == NodeEndpointConfig::IPv4);

            seastar::listen_options lo;
            lo.reuse_address = true;
            _listener = seastar::engine().listen(make_ipv4_address(nodeConfig.ipv4.address, nodeConfig.ipv4.port), lo);

            std::cout << "Start transport for shard #" << engine().cpu_id() << " on endpoint {ipv4:"
                << (((nodeConfig.ipv4.address)>>24)&0xFF) << "." << (((nodeConfig.ipv4.address)>>16)&0xFF)
                << "." << (((nodeConfig.ipv4.address)>>8)&0xFF) << "." << ((nodeConfig.ipv4.address)&0xFF)
                << " port:" << nodeConfig.ipv4.port << "}" << std::endl << std::flush;

            seastar::keep_doing([this]
            {
                TRACE();
                return _listener->accept()
                    .then([this] (connected_socket fd, socket_address addr) mutable
                    {
                        TRACE();
                        seastar::lw_shared_ptr<Connection> connection = seastar::make_lw_shared<Connection>(std::move(fd), addr);
                        return processMessage(connection).then([connection] { return connection->finishRound(); })

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
                        .finally([connection] { TRACE(); return connection->_out.close().finally([connection]{}); });
                    })
                    .handle_exception([] (std::exception_ptr ep)
                    {
                        try
                        {
                            std::rethrow_exception(ep);
                        } catch(const std::exception& e)
                        {
                            std::cout << "Caught exception " << e.what() << std::endl << std::flush;
                            throw e;
                        }
                    });
            }).or_terminate();

            TRACE();
        }

        void startTaskProcessor()
        {
            seastar::keep_doing([this]
            {
                _assignmentManager.processTasks();
                return seastar::make_ready_future<>();
            }).or_terminate();
        }

    public:
        TCPServer(NodePool& nodePool) : _assignmentManager(nodePool) { }

        void start()
        {
            startTransport();
            startTaskProcessor();
        }

        future<> stop() { return make_ready_future<>(); }
    };

public:
    SeastarPlatform() {}

    Status run(NodePool& pool)
    {
        seastar::distributed<TCPServer> server;
        seastar::app_template app;

        // TODO: So far couldn't find a good way to configure Seastar thorugh app. add_options without using command arguments.
        // Will do more research later and fix it. Now just configure through argument
        std::string nodesCountArg = std::string("-c") + std::to_string(pool.getNodesCount());

        const char* argv[] = { "NodePool", nodesCountArg.c_str(), nullptr };
        int argc = sizeof(argv)/sizeof(*argv)-1;

        int ret = app.run_deprecated(
            sizeof(argv)/sizeof(*argv)-1,   // argc
            (char**)argv,                   // argv
            [&]
            {
                engine().at_exit([&] { return server.stop(); });
                return server.start(std::ref(pool))
                    .then([&server] { return server.invoke_on_all(&TCPServer::start); });
            });

        return ret == 0 ? Status::Ok : Status::SchedulerPlatformStartingFailure;
    }
};

} //  namespace k2
