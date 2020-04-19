//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/tso/service/TSOService.h>

int main(int argc, char** argv)
{
    k2::App app;

    app.addApplet<k2::TSOService>();

    return app.start(argc, argv);
}
