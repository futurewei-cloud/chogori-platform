//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include <k2/cpo/service/CPOService.h>

int main(int argc, char** argv) {
    k2::App app;
    app.addOptions()
        ("assignment_timeout", bpo::value<k2::ParseableDuration>(), "Timeout for K2 partition assignment")
        ("heartbeat_deadline", bpo::value<k2::ParseableDuration>(), "K2 Txn heartbeat deadline")
        ("data_dir", bpo::value<k2::String>(), "The directory where we can keep data");
    app.addApplet<k2::CPOService>([]() mutable -> seastar::distributed<k2::CPOService>& {
        return k2::AppBase().getDist<k2::CPOService>();
    });
    return app.start(argc, argv);
}
