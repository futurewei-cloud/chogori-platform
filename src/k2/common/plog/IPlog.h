#pragma once

#include <k2/common/Serialization.h>
#include "plog_client.h"
#include <seastar/core/sharded.hh>
#include <k2/common/Payload.h>

namespace k2
{

struct PlogId : plog_id_t
{
    K2_PAYLOAD_COPYABLE;
};

struct PlogInfo
{
    uint32_t size;
    bool sealed;
};

class RefCountable
{
protected:
    mutable uint64_t refCount = 0;
public:
    void AddRef() const { refCount++; }

    bool Release() const
    {
        if(--refCount)
            return true;

        delete this;    //  TODO: support arenas. Create virtual Destroy function.
        return false;
    }

    virtual ~RefCountable() {}
};

template<class RefCountableT>
class SharedPtr
{
    RefCountableT* ptr = nullptr;
public:
    SharedPtr(const RefCountableT* ptr) : ptr(ptr)
    {
        if(ptr)
            ptr->AddRef();
    }

    SharedPtr(const SharedPtr& sharedPtr) : SharedPtr(sharedPtr.ptr) { }
    SharedPtr(SharedPtr&& sharedPtr) : ptr(sharedPtr.ptr) { sharedPtr.ptr = nullptr; }
    SharedPtr& operator=(const SharedPtr& other)
    {
        if(ptr)
            ptr->Release();

        ptr = other.ptr;
        if(ptr)
            ptr->AddRef();
        return *this;
    }

    SharedPtr& operator=(SharedPtr&& other)
    {
        if(ptr)
            ptr->Release();

        ptr = other.ptr;
        return *this;
    }

    RefCountableT& operator*() const noexcept { return *ptr; }

    RefCountableT* operator->() const noexcept { return ptr; }

    ~SharedPtr()
    {
        if(ptr)
            ptr->Release();
    }
};

template<class IOResultT>
class IOResultBase : public RefCountable
{
public:
    typedef SharedPtr<IOResultT> Ptr;
    typedef std::function<void(const Ptr&)> CallbackT;
protected:
    Status _status = Status::IOOperationHasNotBeenFinished;
    bool _finished = false;
    CallbackT _callback;

    void setResult(Status error)    //  TODO: what if already set?
    {
        _finished = true;
        _status = error;

        if(_callback)
        {
            _callback(Ptr((IOResultT*)this));
            _callback = nullptr;
        }
    }

public:
    void cancel()
    {
        setResult(Status::IOOperationCanceled);
    }

    Status getStatus()
    {
        return _status;
    }

    bool done()
    {
        return _finished;
    }

    void then(CallbackT callback)
    {
        if(_finished)
        {
            callback(Ptr((IOResultT*)this));
            return;
        }

        if(_callback)
        {
            _callback = [oldCallback = std::move(_callback), newCallback = std::move(_callback)](const Ptr& self)
            {
                oldCallback(self);
                newCallback(self);
            };
        }
        else
            _callback = std::move(callback);
    }
};

class ReadRegion
{
public:
    uint32_t  offset;
    uint32_t  size;
    Binary buffer;

public:
    ReadRegion(uint32_t  offset, uint32_t  size) : offset(offset), size(size) { }
    ReadRegion(uint32_t  offset, uint32_t  size, Binary buffer) : offset(offset), size(size), buffer(std::move(buffer)) {}

    DEFAULT_MOVE(ReadRegion);
};

typedef std::vector<ReadRegion> ReadRegions;

//
//  IPlog in simplified Plog interface, containing only essential for K2 function and arguments.
//
class IPlog
{
public:
    //
    //  Allocate group of plogs
    //
    virtual seastar::future<std::vector<PlogId>> create(uint plogCount) = 0;

    //
    //  Return PLOG information
    //
    virtual seastar::future<PlogInfo> getInfo(const PlogId& plogId) = 0;

    //
    //  Append buffers to the end of PLOG. Size of data in buffer cannot exceed 2MB
    //
    virtual seastar::future<uint32_t> append(const PlogId& plogId, Binary buffer) = 0;

    //
    //  Read the region from PLOG
    //
    virtual seastar::future<ReadRegion> read(const PlogId& plogId, ReadRegion region) = 0;

    //
    //  Seal PLOG: make PLOG read-only and finalize the size
    //
    virtual seastar::future<> seal(const PlogId& plogId) = 0;

    //
    //  Drop the PLOG
    //
    virtual seastar::future<> drop(const PlogId& plogId) = 0;

    // close all resources
    virtual seastar::future<> close() = 0;

    //
    //  Helper functions
    //
    seastar::future<Binary> read(const PlogId& plogId, uint32_t offset, uint32_t size)
    {
        return read(plogId, ReadRegion(offset, size, Binary(size))).then([](ReadRegion region)
        {
            return std::move(region.buffer);
        });
    }

    virtual seastar::future<ReadRegions> readMany(const PlogId& plogId, ReadRegions plogDataToReadList)
    {
        std::vector<seastar::future<ReadRegion>> readFutures;
        for(ReadRegion& region : plogDataToReadList)
            readFutures.push_back(read(plogId, std::move(region)));

        return seastar::when_all_succeed(readFutures.begin(), readFutures.end())
            .then([this](std::vector<ReadRegion> regions) mutable { return regions; });
    }

    //
    //  Read all data from Plog into payload
    //
    seastar::future<> readAll(const PlogId& plogId, Payload& payload)
    {
        return getInfo(plogId).then([&payload, plogId, this](PlogInfo info) mutable
        {
            if(!info.size)
                return seastar::make_ready_future<>();

            return seastar::do_with(info.size, uint32_t(0), plogId, [this, &payload](uint32_t& size, uint32_t& offset, PlogId& plogId) mutable
            {
                return seastar::repeat([&size, &offset, &plogId, &payload, this]() mutable
                {
                    if(offset >= size)
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);

                    return read(plogId, offset, std::min(size - offset, (uint32_t)8*1024)).then([&payload, &offset](Binary bin)
                    {
                        offset += bin.size();
                        payload.appendBinary(std::move(bin));
                        return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                    });
                });
            });
        });
    }

    seastar::future<Payload> readAll(const PlogId& plogId)
    {
        return seastar::do_with(Payload(nullptr), [this, plogId] (auto& payload) mutable
        {
            return readAll(plogId, payload).then([&payload] { return seastar::make_ready_future<Payload>(std::move(payload)); });
        });
    }

    virtual seastar::future<uint32_t> appendMany(const PlogId& plogId, std::vector<Binary> bufferList)
    {
        size_t totalSize = 0;
        for(Binary& bin : bufferList)
            totalSize += bin.size();

        Binary buffer(totalSize);
        char* ptr = buffer.get_write();
        for(Binary& bin : bufferList)
        {
            std::memcpy(ptr, bin.get(), bin.size());
            ptr += bin.size();
        }

        return append(plogId, std::move(buffer));
    }

    seastar::future<uint32_t> append(const PlogId& plogId, const void* buffer, size_t bufferSize)
    {
        return append(plogId, Binary((const char*)buffer, bufferSize));
    }

    seastar::future<uint32_t> getSize(const PlogId& plogId) { return getInfo(plogId).then([this](PlogInfo info) { return info.size; }); }
    seastar::future<bool> isSealed(const PlogId& plogId) { return getInfo(plogId).then([this](PlogInfo info) { return info.sealed; }); }

    seastar::future<PlogId> createOne()
    {
        return create(1).then([this](std::vector<PlogId> plogIds) { return plogIds[0]; });
    }

};  //  class IPlog

}   //  namespace k2
