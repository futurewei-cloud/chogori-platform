// draft compilable
#include <string>
#include <unordered_map>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>

#include <k2/common/Common.h>
#include <k2/common/Log.h>
namespace ss=seastar;

class Platform;
class AppBase {
   public:
    k2::String name;
    virtual void init() = 0;
    AppBase(k2::String&& name) : name(name) {}
    virtual ~AppBase() {}
    Platform* platform;
};

class Platform {
public:
    void getDispatcher(){}
    void getScheduler(){}
    void getConfig(){}
    template <typename T>
    T* getApp(const k2::String& name) {
        return static_cast<T*>(_apps[name].get());
    }

   protected:
    std::unordered_map<k2::String, ss::shared_ptr<AppBase>> _apps;
};

class Container: public Platform {
public:
    Container(int argc, char** argv){
        (void)argc;
        (void)argv;
    }
    void addApp(ss::shared_ptr<AppBase> app) {
        _apps[app->name] = app;
        app->platform = this;
    }
    void start(){
        for (auto&x:_apps) {
            K2INFO("Initializing app: " << x.first);
            x.second->init();
        }
    }
};

class Benchmark: public AppBase {
public:
    // distributed version of the class
    typedef seastar::distributed<Benchmark> Dist_t;
    Benchmark() : AppBase("Benchmark"), field("val1") {}
    virtual void init() override {
        K2INFO("Benchmark initialize");
        K2INFO("Field=" << platform->getApp<Benchmark>("Benchmark"));
    }
    k2::String field;
};

int main(int argc, char** argv) {
    Container c(argc, argv);
    c.addApp(ss::static_pointer_cast<AppBase>(ss::make_shared<Benchmark>()));

    c.start();
}
