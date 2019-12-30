//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include <k2/tso/service/TSOService.h>

int main(int argc, char** argv) {
    k2::App app;
    // pass the ss::distributed container to the TSOService constructor
    app.addApplet<k2::TSOService>(seastar::ref(app.getDist<k2::TSOService>()));
    return app.start(argc, argv);
}
