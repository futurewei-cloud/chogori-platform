#include <seastar/net/ip.hh>
#include <seastar/net/tcp.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include "common/Log.h"
#include "transport/BaseTypes.h"
#define CLOG(msg) {K2INFO("{conn="<< (void*)this << ", addr=" << this->_addr << "} " << msg)}
class connection {
    seastar::connected_socket _tcp_conn;
    seastar::socket_address _addr;
    // the input() and output() methods on connected_socket return new input/output wrappers
    // for the underlying streams each time you call them. so we have to remember them here
    seastar::input_stream<char> _in;
    seastar::output_stream<char> _out;
public:
    explicit connection(seastar::connected_socket tc, seastar::socket_address addr):
        _tcp_conn(std::move(tc)),
        _addr(std::move(addr)),
        _in(_tcp_conn.input()),
        _out(_tcp_conn.output()) {
        CLOG("ctor");
    }

    seastar::future<> send(seastar::temporary_buffer<char> packet) {
        return _out.write(std::move(packet)).then([this]() {
            return this->_out.flush();
        });
    }

    ~connection() {
        CLOG("Conn close")
    }

    void run() {
        seastar::do_until(
            [this] { return this->_in.eof(); }, // end condition for loop
            [this] { // body of loop
                return this->_in.read().then(
                    [this](seastar::temporary_buffer<char> packet) {
                        if (packet.empty()) {
                            K2INFO("remote end closed connection");
                            assert(this->_in.eof()); // at this point _in.eof() should be true
                            return; // just say we're done so the loop can evaluate the end condition
                        }
                        K2INFO("Read "<< packet.size() << " bytes: " << k2::String(packet.get(), packet.size()));
                        this->send(std::move(packet));
                    });
            }
        )
        .finally([this] {
            CLOG("asked to close")
            return this->_in.close().then([this]() {
                CLOG("closed input")
                return _out.close();})
                .finally([this]{
                    CLOG("closed output")
                    delete this;
                });
        });
    }
}; // class connection


class Server {
    seastar::lw_shared_ptr<seastar::server_socket> _listen_socket;
    bool _stopped;
public:
    Server() {
        K2INFO("Server ctor");
        seastar::listen_options lopts;
        lopts.reuse_address = true;
        _listen_socket = seastar::engine().listen(seastar::make_ipv4_address({10000}), lopts);
        _stopped = false;
    }

    void run() {
        K2INFO("Running Server");
        seastar::do_until(
            [this] { return _stopped;},
            [this] {
            return _listen_socket->accept().then([this] (seastar::connected_socket fd, seastar::socket_address addr) mutable {
                K2INFO("Accepted connection from " << addr);
                ( new connection(std::move(fd), std::move(addr)) )->run();
            })
            .handle_exception([this] (auto exc) {
                if (!_stopped) {
                    K2INFO("Accept received exception(ignoring): " << exc);
                }
                else {
                    K2INFO("Server is exiting...");
                }
                return seastar::make_ready_future();
            });
        }).or_terminate();
    }

    seastar::future<> stop() {
        K2INFO("Server stop");
        _stopped = true;
        _listen_socket->abort_accept();
        return seastar::make_ready_future();
    }
}; // struct Server

int main(int argc, char** argv) {
    seastar::distributed<Server> server;
    seastar::app_template app;
    return app.run_deprecated(argc, argv, [&] {
        seastar::engine().at_exit([&] {
            K2INFO("Engine exit");
            return server.stop();
        });
        server.start().then([&]() {
            K2INFO("Service started...");
            server.invoke_on_all(&Server::run);
        });
    });
    K2INFO("main exit");
}
