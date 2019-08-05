#pragma once

#include "common/Serialization.h"
#include "plog_client.h"
#include <seastar/core/sharded.hh>
#include "common/Payload.h"

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

/*
template<typename ResultT>
class IOResult : public IOResultBase<IOResult>
{
public:
    ResultT result;
};
*/

template<typename... ResultT>
using IOResult = seastar::future<ResultT...>;

//
//  IPlog in simplified Plog interface, containing only essential for K2 function and arguments.
//
class IPlog
{
public:
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
    //  Allocate group of plogs
    //
    virtual IOResult<std::vector<PlogId>> create(uint plogCount) = 0;

    //
    //  Return PLOG information
    //
    virtual IOResult<PlogInfo> getInfo(const PlogId& plogId) = 0;

    //
    //  Append buffers to the end of PLOG. Size of all data in buffers cannot exceed 2MB
    //
    virtual IOResult<uint32_t> append(const PlogId& plogId, std::vector<Binary> bufferList) = 0;

    //
    //  Read the region from PLOG
    //
    virtual IOResult<ReadRegions> read(const PlogId& plogId, ReadRegions plogDataToReadList) = 0;

    //
    //  Seal PLOG: make PLOG read-only and finalize the size
    //
    virtual IOResult<> seal(const PlogId& plogId) = 0;

    //
    //  Drop the PLOG
    //
    virtual IOResult<> drop(const PlogId& plogId) = 0;

    //
    //  Helper functions
    //
    IOResult<Binary> read(const PlogId& plogId, uint32_t offset, uint32_t size)
    {
        ReadRegions regions;
        regions.emplace_back(offset, size, k2::Binary(size));
        return read(plogId, std::move(regions)).then([](ReadRegions regions)
        {
            return std::move(regions[0].buffer);
        });
    }

    //
    //  Read all data from Plog into payload
    //
    IOResult<> readAll(const PlogId& plogId, Payload& payload)
    {
        return getInfo(plogId).then([&payload, plogId, this](PlogInfo info) mutable
        {
            return seastar::do_with(info.size, uint32_t(0), plogId, [this, &payload](uint32_t& size, uint32_t& offset, PlogId& plogId) mutable
            {
                return seastar::repeat([&size, &offset, &plogId, &payload, this]() mutable
                {
                    if(offset >= size)
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);

                    return read(plogId, offset, std::max(size - offset, (uint32_t)8*1024)).then([&payload](Binary bin)
                    {
                        payload.appendBinary(std::move(bin));
                        return  seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                    });
                });
            });
        });
    }
};  //  class IPlog

}   //  namespace k2
