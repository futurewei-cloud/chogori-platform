//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include <k2/cpo/service/CPOService.h>

int main(int argc, char** argv) {
    k2::App app;
    app.addOptions()
        ("data_dir", bpo::value<std::string>(), "The directory where we can keep data");
    app.addApplet<k2::CPOService>([&]() mutable -> seastar::distributed<k2::CPOService>& {
        return app.getDist<k2::CPOService>();
    });
    return app.start(argc, argv);
}
