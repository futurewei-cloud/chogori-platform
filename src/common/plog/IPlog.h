#pragma once

#include "common/Common.h"
#include "plog_client.h"
#include <seastar/core/sharded.hh>

namespace k2
{

typedef plog_id_t PlogId;

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
    };

    typedef std::vector<ReadRegion> ReadRegions;

    virtual IOResult<std::vector<PlogId>> create(uint plogCount) = 0;

    virtual IOResult<PlogInfo> getInfo(const PlogId& plogId) = 0;

    virtual IOResult<uint32_t> append(const PlogId& plogId, std::vector<Binary> bufferList) = 0;

    virtual IOResult<ReadRegions> read(const PlogId& plogId, ReadRegions plogDataToReadList) = 0;

    virtual IOResult<> seal(const PlogId& plogId) = 0;

    virtual IOResult<> drop(const PlogId& plogId) = 0;
};  //  class IPlog

}   //  namespace k2
