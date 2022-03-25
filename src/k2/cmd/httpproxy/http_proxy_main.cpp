/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/httpproxy/http_proxy.h>

int main(int argc, char** argv) {
    k2::App app("K23SIBenchProxy");
    app.addApplet<k2::APIServer>();
    app.addApplet<k2::tso::TSOClient>();
    app.addApplet<k2::HTTPProxy>();
    app.addOptions()
        // config for dependencies
        ("num_partitions", bpo::value<uint32_t>(), "Number of k2 nodes to use for the collection")
        ("partition_request_timeout", bpo::value<k2::ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo_request_timeout", bpo::value<k2::ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<k2::ParseableDuration>(), "CPO request backoff");
    return app.start(argc, argv);
}
