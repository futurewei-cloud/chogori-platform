//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include <k2/tso/service/TSOService.h>

int main(int argc, char** argv) {
    k2::App<k2::TSOService> app;

    // pass the ss::distributed container to the TSOService constructor
    return app.start(argc, argv, seastar::ref(app.getDist()));
}