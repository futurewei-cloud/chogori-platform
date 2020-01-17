//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#include <k2/appbase/Appbase.h>
#include "CPOTest.h"

int main(int argc, char** argv) {
    k2::App app;
    app.addOptions()("cpo_endpoint", bpo::value<std::string>(), "The endpoint of the CPO service");
    app.addApplet<CPOTest>();
    return app.start(argc, argv);
}
