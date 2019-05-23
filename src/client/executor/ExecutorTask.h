#pragma once

namespace k2
{

// forward declaration
class ExecutorTask;

// function which is invoked to propagate the response back to the client
typedef std::function<void(std::unique_ptr<ResponseMessage>)> ResponseCallback;
// function which is invoked by the transport platform to populate the payload ptr for the request
typedef std::function<void(std::unique_ptr<Payload>)> PayloadPtrCallback;
// function which is invoked by the transport platform to populate the payload reference for the request
typedef std::function<void(Payload&)> PayloadRefCallback;
// shared pointer for the executor task
typedef seastar::lw_shared_ptr<ExecutorTask> ExecutorTaskPtr;

//
// Task representing a message that needs to be sent to a K2 endpoint.
//
// Memory management can get a bit complex here, so the responsibilities are divided as following:
// - The client thread is responsible for the lifecycle of the task object and all the members, except from the payload and the response.
// - The transport platform is responsible for the lifecycle of the payload and the response.
//
struct ExecutorTask
{
    //
    // Data for which the transport platform is responsible for.
    //
    struct PlatformData
    {
        std::unique_ptr<k2::TXEndpoint> _pEndpoint;
        std::unique_ptr<ResponseMessage> _pResponse;
        std::unique_ptr<Payload> _pPayload;
    };

    //
    // Data for which the client thread is responsible for.
    //
    struct ClientData
    {
        String _url;
        std::unique_ptr<Payload> _pPayload;
        k2::Duration _timeout;
        PayloadRefCallback _fPayloadRef = nullptr;
        PayloadPtrCallback _fPayloadPtr = nullptr;
        ResponseCallback _fResponse = nullptr;

        ClientData(String url, Duration timeout, PayloadRefCallback& fPayloadRef, ResponseCallback& fResponse)
            : _url(std::move(url))
            , _timeout(std::move(timeout))
            , _fPayloadRef(fPayloadRef)
            , _fResponse(fResponse)
        {
            // empty
        }

        ClientData(String url, PayloadPtrCallback fPayloadPtr)
            : _url(std::move(url))
            , _fPayloadPtr(fPayloadPtr)
        {
            // empty
        }

        ClientData(String url, std::unique_ptr<Payload> pPayload, Duration timeout, ResponseCallback& fResponse)
            : _url(std::move(url))
            , _pPayload(std::move(pPayload))
            , _timeout(timeout)
            , _fResponse(fResponse)
        {
            // empty
        }
     };

    std::unique_ptr<ClientData> _pClientData;
    // we do not want this object to be deleted when the destructor of the task is invoked
    std::unique_ptr<PlatformData> _pPlatformData;

    String& getUrl()
    {
        return _pClientData->_url;
    }

    Duration& getTimeout()
    {
        return _pClientData->_timeout;
    }

    bool shouldCreatePayload()
    {
        return (_pClientData->_fPayloadPtr) || (_pClientData->_fPayloadRef);
    }

    bool hasResponseCallback()
    {
        return _pClientData->_fResponse != nullptr;
    }

    bool hasEndpoint()
    {
        return _pPlatformData->_pEndpoint.get();
    }

    void invokePayloadCallback()
    {
        if(_pClientData->_fPayloadPtr) {
            (_pClientData->_fPayloadPtr)(std::move(_pPlatformData->_pPayload));
        }
        else {
            (_pClientData->_fPayloadRef)(*_pPlatformData->_pPayload);
        }
    }

    void invokeResponseCallback()
    {
        (_pClientData->_fResponse)(std::move(_pPlatformData->_pResponse));
    }
};

}; // namespace k2
