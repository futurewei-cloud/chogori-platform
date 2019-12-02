//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <appbase/Appbase.h>
#include <tso/service/TSOService.h>

int main(int argc, char** argv) {
    k2::App<k2::TSOService> app;

    return app.start(argc, argv);
}
