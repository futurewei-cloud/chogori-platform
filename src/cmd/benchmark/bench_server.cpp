/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014 Cloudius Systems
 */

#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/print.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <vector>

using namespace seastar;

static int msg_size;
static int tx_msg_nr = 90000000;
static std::string str_txbuf;
static bool enable_tcp = false;
static bool enable_sctp = false;
static std::string test;

class tcp_server
{
    std::vector<server_socket> _tcp_listeners;
    std::vector<server_socket> _sctp_listeners;

public:
    future<> listen(ipv4_addr addr)
    {
        if (enable_tcp) {
            listen_options lo;
            lo.proto = transport::TCP;
            lo.reuse_address = true;
            _tcp_listeners.push_back(
                engine().listen(make_ipv4_address(addr), lo));
            do_accepts(_tcp_listeners);
        }

        if (enable_sctp) {
            listen_options lo;
            lo.proto = transport::SCTP;
            lo.reuse_address = true;
            _sctp_listeners.push_back(
                engine().listen(make_ipv4_address(addr), lo));
            do_accepts(_sctp_listeners);
        }
        return make_ready_future<>();
    }

    // FIXME: We should properly tear down the service here.
    future<> stop() { return make_ready_future<>(); }

    void do_accepts(std::vector<server_socket> &listeners)
    {
        int which = listeners.size() - 1;
        listeners[which]
            .accept()
            .then([this, &listeners](connected_socket fd,
                                     socket_address addr) mutable {
                auto conn = new connection(*this, std::move(fd), addr);
                conn->process().then_wrapped([conn](auto &&f) {
                    delete conn;
                    try {
                        f.get();
                    } catch (std::exception &ex) {
                        std::cout << "request error " << ex.what() << "\n";
                    }
                });
                do_accepts(listeners);
            })
            .then_wrapped([](auto &&f) {
                try {
                    f.get();
                } catch (std::exception &ex) {
                    std::cout << "accept failed: " << ex.what() << "\n";
                }
            });
    }
    class connection
    {
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;

    public:
        connection(tcp_server &server, connected_socket &&fd,
                   socket_address addr)
            : _fd(std::move(fd)), _read_buf(_fd.input()),
              _write_buf(_fd.output())
        {
        }
        future<> process() { return read(); }
        future<> read()
        {
            if (_read_buf.eof()) {
                return make_ready_future();
            }

            // pingpong test
            if (test == "ping") {
                return _read_buf.read_exactly(msg_size).then(
                    [this](temporary_buffer<char> buf) {
                        auto str_rxbuf = std::string(buf.get(), buf.size());
                        // Need to trick compiler?
                        if (str_rxbuf == "EXIT") {
                            return make_ready_future();
                        }

                        return _write_buf.write(str_txbuf)
                            .then([this] { return _write_buf.flush(); })
                            .then([this] { return this->read(); });
                    });
            }

            // server tx test
            return rx_test();
        }
        future<> do_write(int end)
        {
            if (end == 0) {
                return make_ready_future<>();
            }
            return _write_buf.write(str_txbuf)
                .then([this] { return _write_buf.flush(); })
                .then([this, end] { return do_write(end - 1); });
        }

        future<> do_read()
        {
            return _read_buf.read_exactly(msg_size).then(
                [this](temporary_buffer<char> buf) {
                    if (buf.size() == 0) {
                        return make_ready_future();
                    } else {
                        return do_read();
                    }
                });
        }

        future<> rx_test()
        {
            return do_read().then([] { return make_ready_future<>(); });
        }
    };
};

namespace bpo = boost::program_options;

int main(int ac, char **av)
{
    app_template app;
    app.add_options()("port", bpo::value<uint16_t>()->default_value(10000),
                      "TCP server port")(
        "tcp", bpo::value<std::string>()->default_value("yes"), "tcp listen")(
        "test", bpo::value<std::string>()->default_value("ping"),
        "test type(ping | rxrx)")("msg_size",
                                  bpo::value<int>()->default_value(64),
                                  "Size in bytes of each message")(
        "sctp", bpo::value<std::string>()->default_value("no"), "sctp listen");

    return app.run_deprecated(ac, av, [&] {
        auto &&config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        enable_tcp = config["tcp"].as<std::string>() == "yes";
        enable_sctp = config["sctp"].as<std::string>() == "yes";
        test = config["test"].as<std::string>();
        msg_size = config["msg_size"].as<int>();
        str_txbuf = std::string(msg_size, 'X');

        if (!enable_tcp && !enable_sctp) {
            fprint(std::cerr, "Error: no protocols enabled. Use \"--tcp yes\" "
                              "and/or \"--sctp yes\" to enable\n");
            return engine().exit(1);
        }
        auto server = new distributed<tcp_server>;
        server->start()
            .then([server = std::move(server), port]() mutable {
                engine().at_exit([server] { return server->stop(); });
                server->invoke_on_all(&tcp_server::listen, ipv4_addr{port});
            })
            .then([port] {
                std::cout << "Seastar TCP server listening on port " << port
                          << " ...\n";
            });
    });
}
