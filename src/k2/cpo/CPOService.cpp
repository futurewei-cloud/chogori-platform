#include "CPOService.h"
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/Payload.h>  // for payload construction
#include <k2/transport/Status.h>  // for RPC
#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/dto/ControlPlaneOracle.h> // our DTO
#include <k2/dto/MessageVerbs.h> // our DTO

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

namespace k2 {

CPOService::CPOService(DistGetter distGetter) : _dist(distGetter) {
    K2INFO("ctor");
}

CPOService::~CPOService() {
    K2INFO("dtor");
}

seastar::future<> CPOService::stop() {
    K2INFO("stop");
    return seastar::make_ready_future<>();
}

seastar::future<> CPOService::start() {
    K2INFO("Registering message handlers");
    RPC().registerRPCObserver<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE, [this](dto::CollectionCreateRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleCreate, std::move(request));
    });

    RPC().registerRPCObserver<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET, [this](dto::CollectionGetRequest&& request) {
        return _dist().invoke_on(0, &CPOService::handleGet, std::move(request));
    });

    _dataDir = Config()["data_dir"].as<std::string>();
    if (seastar::engine().cpu_id() == 0) {
        // only core 0 handles CPO business
        if (mkdir(_dataDir.c_str(), 0777 != 0)) {
            if (errno != EEXIST) {
                K2ERROR("Unable to create data directory: " << strerror(errno));
                throw std::runtime_error("unable to create data directory");
            }
            K2INFO("Using existing data directory: " << _dataDir);
        }
        else {
            K2INFO("Using data directory: " << _dataDir);
        }
    }
    return seastar::make_ready_future<>();
}

seastar::future<std::tuple<Status, dto::CollectionCreateResponse>>
CPOService::handleCreate(dto::CollectionCreateRequest&& request) {
    K2INFO("Received collection create request for " << request.metadata.name);

    auto cpath = _getCollectionPath(request.metadata.name);
    { // see if file is there.
        int fd = ::open(cpath.c_str(), O_RDONLY);
        if (fd >= 0) {
            ::close(fd);
            return RPCResponse(Status::S403_Forbidden("Collection already exists"), dto::CollectionCreateResponse());
        }
    }
    // create a collection from the incoming request
    auto collection = dto::Collection();
    collection.metadata = request.metadata;
    Payload p([] { return Binary(4096); });
    p.write(collection);
    p.truncateToCurrent();
    auto leftBytes = p.getSize();

    int fd = ::open(cpath.c_str(), O_CREAT | O_WRONLY);
    if (fd < 0) {
        K2ERROR("Unable to open collection for writing: name=" << cpath << ", err=" << strerror(errno));
        throw std::runtime_error("unable to create collection");
    }

    for(auto&&buf: p.release()) {
        auto towrite = std::min(buf.size(), leftBytes);
        size_t written = ::write(fd, buf.get(), towrite);
        if (written != towrite) {
            ::close(fd);
            throw std::runtime_error("unable to write collection");
        }
        leftBytes -= written;
        if (leftBytes == 0) break;
    }
    ::close(fd);
    K2INFO("Created collection: " << cpath);
    return RPCResponse(Status::S201_Created(), dto::CollectionCreateResponse());
}

seastar::future<std::tuple<Status, dto::CollectionGetResponse>>
CPOService::handleGet(dto::CollectionGetRequest&& request) {
    K2INFO("Received collection get request for " << request.name);
    auto cpath = _getCollectionPath(request.name);
    int fd = ::open(cpath.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return RPCResponse(Status::S404_Not_Found("Collection not found"), dto::CollectionGetResponse());
        }
        K2ERROR("problem opening collection: name= " << cpath << ":: " << strerror(errno));
        throw std::runtime_error("unable to open collection file");
    }

    Payload p;
    while (1) {
        Binary buf(4096);
        auto rd = ::read(fd, buf.get_write(), buf.size());
        if (rd < 0) {
            K2ERROR("problem reading collection: name= " << cpath << ":: " << strerror(errno));
            throw std::runtime_error("unable to read collection file");
        }
        buf.trim(rd);
        p.appendBinary(std::move(buf));
        if (rd == 0) break;
    }
    ::close(fd);
    dto::CollectionGetResponse result;
    if (!p.read(result.collection)) {
        return RPCResponse(Status::S500_Internal_Server_Error("Unable to read collection data"), dto::CollectionGetResponse());
    };
    K2INFO("Found collection in: " << cpath);
    return RPCResponse(Status::S200_OK(), std::move(result));
}

String CPOService::_getCollectionPath(String name) {
    return _dataDir + "/" + name + ".collection";
}

} // namespace k2
