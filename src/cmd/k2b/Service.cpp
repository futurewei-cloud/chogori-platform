// seastar
#include <seastar/core/app-template.hh>
// k2b
#include <benchmarker/Benchmarker.h>

int main(int argc, char** argv) {

    seastar::app_template app;
    return app.run_deprecated(argc, argv, [&] {
        return seastar::make_ready_future<>();
    });
}
