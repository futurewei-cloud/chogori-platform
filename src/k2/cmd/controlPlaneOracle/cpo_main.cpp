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

#include <k2/appbase/Appbase.h>
#include <k2/infrastructure/APIServer.h>
#include <k2/cpo/service/CPOService.h>

int main(int argc, char** argv) {
    k2::App app("CPOService");
    app.addOptions()
        ("assignment_timeout", bpo::value<k2::ParseableDuration>(), "Timeout for K2 partition assignment")
        ("heartbeat_deadline", bpo::value<k2::ParseableDuration>(), "K2 Txn heartbeat deadline")
        ("data_dir", bpo::value<k2::String>(), "The directory where we can keep data");
    app.addApplet<k2::CPOService>([]() mutable -> seastar::distributed<k2::CPOService>& {
        return k2::AppBase().getDist<k2::CPOService>();
    });
    app.addApplet<k2::APIServer>();
    return app.start(argc, argv);
}
