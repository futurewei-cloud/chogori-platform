//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
// stl
#include <cstdint> // for int types
#include <queue>

// k2
#include "RPCTypes.h"
#include "RPCHeader.h"
#include <k2/common/Common.h>
#include <k2/common/Log.h>
#include "Payload.h"
#include "Status.h"

namespace k2 {

// This class
class RPCParser {
public: // types
    // The type for Message observers
    typedef std::function<void(Verb verb, MessageMetadata metadata, std::unique_ptr<Payload> payload)> MessageObserver_t;

    // the type for parser failure observer
    typedef std::function<void(std::exception_ptr exc)> ParserFailureObserver_t;

    // indicates the message failed to validate magic bits
    class MagicMismatchException : public std::exception {};

    // indicates that we expected to receive the second segment for partial header, but
    // the segment we received did not have enough data.
    class NonContinuationSegmentException : public std::exception {};

public:
    // creates an RPC parser with the given preemptor function
    inline RPCParser(std::function<bool()> preemptor);

    // destructor. Any incomplete messages are dropped
    inline ~RPCParser();

    // Utility method used to create a header for an outgoing message
    // the incoming binary must have exactly txconstants::MAX_HEADER_SIZE bytes
    // reserved in the beginning.
    // the incoming binary is is populated with the header and trimmed from the front to the first
    // byte of the header
    // returns the number of bytes written as header
    inline static size_t serializeHeader(Binary& binary, Verb verb, MessageMetadata metadata);

    //
    //  Write transport header to the binary. Return false, if not enough space in binary.
    //
    inline static bool writeHeader(Binary& binary, Verb verb, MessageMetadata meta, size_t* written = nullptr);

    // Utility method used to serialize the given user message into a transport message, expressed as a Payload.
    // The user can also provide features via the metadata field
    inline static std::unique_ptr<Payload> serializeMessage(Payload&& message, Verb verb, MessageMetadata metadata);

    // This method should be called with the binary in a stream of messages.
    // we handle messages which can span multiple binarys in this class.
    // The user should use the methods CanDispatch() to determine if it should call DispatchSome()
    // For performance reasons, you should only feed more data once all current data has been processed
    // this method will assert that it is not being called when CanDispatch() is true
    // see usage in TCPRPCChannel.cpp for example on how to setup a processing loop
    inline void feed(Binary&& binary);

    // Use to determine if this parser could potentially dispatch some messages. It is possible that
    // in some cases no messages will be dispatched if DispatchSome() is called
    inline bool canDispatch() { return _shouldParse;}

    // Ask the parser to process data in the incoming binarys and dispatch some messages. This method
    // dispatches 0 or more messages.
    // Under the covers, we consult the preemptor function to stop dispatching even if we have more messages
    // so the user should keep calling DispatchSome until CanDispatch returns false
    inline void dispatchSome();

    // Call this method with a callback to observe incoming RPC messages
    inline void registerMessageObserver(MessageObserver_t messageObserver);

    // Call this method with a callback to observe parsing failure
    inline void registerParserFailureObserver(ParserFailureObserver_t parserFailureObserver);

private: // types
    enum ParseState: uint8_t {
        WAIT_FOR_FIXED_HEADER, // we're waiting for a header for a new message
        IN_PARTIAL_FIXED_HEADER, // we got parts of the fixed header but not all (binaryed fixed header)
        WAIT_FOR_VARIABLE_HEADER, // we got the fixed header, and now we need the variable fields
        IN_PARTIAL_VARIABLE_HEADER, // we got parts of the variable header but not all (binaryed variable header)
        WAIT_FOR_PAYLOAD, // we're waiting for payload
        READY_TO_DISPATCH, // we have a full message ready to be dispatched
        FAILED_STREAM // this is an error state. If we fail to parse a message, we'll be stuck here
    };

private: // methods
    // parses and dispatches one message if possible
    inline void _parseAndDispatchOne();

    // state machine handlers
    inline void _stWAIT_FOR_FIXED_HEADER();
    inline void _stIN_PARTIAL_FIXED_HEADER();
    inline void _stWAIT_FOR_VARIABLE_HEADER();
    inline void _stIN_PARTIAL_VARIABLE_HEADER();
    inline void _stWAIT_FOR_PAYLOAD();
    inline void _stREADY_TO_DISPATCH();
    inline void _stFAILED_STREAM();

private: // fields
    // message observer
    MessageObserver_t _messageObserver;

    // parser failure observer
    ParserFailureObserver_t _parserFailureObserver;

    // this holds the exception which indicates the parser failure type
    std::exception _parserFailureException;

    // needed in our state machine to keep the parsing going.
    // Usually we stop parsing once we dispatch a whole message,
    // or if we need more data to be fed to assemble a whole message
    bool _shouldParse;

    // the parser state
    ParseState _pState;

    // the fixed header for the current message
    FixedHeader _fixedHeader;

    // the metadata for the current message
    MessageMetadata _metadata;

    // the payload for the current message;
    std::unique_ptr<Payload> _payload;

    // partial binary left over from previous parsing. Only used when header(variable or fixed) spans two binarys
    Binary _partialBinary;

    // current incoming binary
    Binary _currentBinary;

    std::function<bool()> _preemptor;

private: // don't need
    RPCParser(const RPCParser& o) = delete;
    RPCParser(RPCParser&& o) = delete;
    RPCParser& operator=(const RPCParser& o) = delete;
    RPCParser& operator=(RPCParser&& o) = delete;
}; // class RPCParser

inline
RPCParser::RPCParser(std::function<bool()> preemptor):
    _shouldParse(false),
    _pState(ParseState::WAIT_FOR_FIXED_HEADER),
    _preemptor(preemptor) {
    K2DEBUG("ctor");
    registerMessageObserver(nullptr);
    registerParserFailureObserver(nullptr);
}

inline
RPCParser::~RPCParser(){
    K2DEBUG("dtor");
}

inline
size_t RPCParser::serializeHeader(Binary& binary, Verb verb, MessageMetadata meta) {
    // we need to write a header of this many bytes:
    auto headerSize = sizeof(FixedHeader) + meta.wireByteCount();
    assert(txconstants::MAX_HEADER_SIZE >= headerSize); // make sure our headers haven't gotten too big
    assert(binary.size() >= txconstants::MAX_HEADER_SIZE); // make sure we have the room

    K2DEBUG("serialize header. Need bytes: " << headerSize);
    // trim the header binary so that it starts at the first header byte
    binary.trim_front(txconstants::MAX_HEADER_SIZE - headerSize);
    size_t writeOffset = 0;

    auto rcode = writeHeader(binary, verb, meta, &writeOffset);
    assert(rcode);

    return headerSize;
}

//
//  Write header of the
//
inline
bool RPCParser::writeHeader(Binary& binary, Verb verb, MessageMetadata meta, size_t* written) {
    size_t writeOffset = 0;

    // take care of the fixed header first
    FixedHeader fHeader;
    fHeader.features=meta.features;
    fHeader.verb=verb;

    if(!appendRaw(binary, writeOffset, fHeader))
        return false;

    // now for variable stuff
    if (meta.isPayloadSizeSet()) {
        K2DEBUG("have payload");
        if(!appendRaw(binary, writeOffset, meta.payloadSize))
            return false;
    }
    if (meta.isRequestIDSet()) {
        K2DEBUG("have request id" << meta.requestID);
        if(!appendRaw(binary, writeOffset, meta.requestID))
            return false;
    }
    if (meta.isResponseIDSet()) {
        K2DEBUG("have response id" << meta.responseID);
        if(!appendRaw(binary, writeOffset, meta.responseID))
            return false;
    }

    // all done.
    K2DEBUG("Write offset after writing header: " << writeOffset);

    if(written)
        *written = writeOffset;

    return true;
}

inline
void RPCParser::registerMessageObserver(MessageObserver_t messageObserver) {
    K2DEBUG("register message observer");
    if (messageObserver == nullptr) {
        K2DEBUG("registering default observer");
        _messageObserver = [](Verb verb, MessageMetadata, std::unique_ptr<Payload>) {
            K2WARN("Dropping message: "<< verb << " since there is no observer registered");
        };
    }
    else {
        K2DEBUG("registering observer");
        _messageObserver = messageObserver;
    }
}

inline
void RPCParser::registerParserFailureObserver(ParserFailureObserver_t parserFailureObserver) {
    K2DEBUG("register parser failure observer");
    if (parserFailureObserver == nullptr) {
        K2DEBUG("registering default parser failure observer");
        _parserFailureObserver = [](std::exception_ptr) {
            K2WARN("parser stream failure ocurred, but there is no observer registered");
        };
    }
    else {
        K2DEBUG("registering parser failure observer");
        _parserFailureObserver = parserFailureObserver;
    }
}

inline
void RPCParser::feed(Binary&& binary) {
    K2DEBUG("feed bytes" << binary.size());
    assert(_currentBinary.empty());

    // always move the incoming packet into the current binary. If there was any partial data left from
    // previous parsing round, it would be in the _partialBinary binary.
    _currentBinary = std::move(binary);
    _shouldParse = true; // signal the parser that we should continue/start parsing data
}

inline
void RPCParser::dispatchSome() {
    K2DEBUG("dispatch some: " << canDispatch());
    while(canDispatch()) {
        // parse and dispatch the next message
        _parseAndDispatchOne();

        if (_preemptor && _preemptor()) {
            K2DEBUG("we hogged the event loop enough");
            break;
        }
    }
}

inline
void RPCParser::_parseAndDispatchOne() {
    // keep going through the motions while we can still keep parsing data
    // or we've dispatched a message
    K2DEBUG("Pado : " << _pState);
    bool dispatched = false;
    while(!dispatched && _shouldParse) {
        K2DEBUG("Parsing in state: " << _pState);
        switch (_pState) {
            case ParseState::WAIT_FOR_FIXED_HEADER: {
                _stWAIT_FOR_FIXED_HEADER();
                break;
            }
            case ParseState::IN_PARTIAL_FIXED_HEADER: {
                _stIN_PARTIAL_FIXED_HEADER();
                break;
            }
            case ParseState::WAIT_FOR_VARIABLE_HEADER: {
                _stWAIT_FOR_VARIABLE_HEADER();
                break;
            }
            case ParseState::IN_PARTIAL_VARIABLE_HEADER: {
                _stIN_PARTIAL_VARIABLE_HEADER();
                break;
            }
            case ParseState::WAIT_FOR_PAYLOAD: {
                _stWAIT_FOR_PAYLOAD();
                break;
            }
            case ParseState::READY_TO_DISPATCH: {
                _stREADY_TO_DISPATCH();
                dispatched = true;
                break;
            }
            case ParseState::FAILED_STREAM: {
                _stFAILED_STREAM();
                break;
            }
            default: {
                assert(false && "Unknown parser state");
                break;
            }
        }
    }
}

inline
void RPCParser::_stWAIT_FOR_FIXED_HEADER() {
    // we come to this state when we think we have enough data to parse a new message from
    // the current binary.
    _payload.reset(); // get rid of any previous payload

    K2DEBUG("wait_for_fixed_header");
    if (_currentBinary.size() == 0) {
        K2DEBUG("wait_for_fixed_header: empty binary");
        _shouldParse = false; // stop trying to parse
        return; // nothing to do - no new data, so remain in this state waiting for new data
    }

    if (_currentBinary.size() < sizeof(_fixedHeader)) {
        K2DEBUG("wait_for_fixed_header: not enough data");
        // we have some new data, but not enough to parse the fixed header. move it to the partial segment
        // and setup for state change to IN_PARTIAL_FIXED_HEADER
        _partialBinary = std::move(_currentBinary);
        _shouldParse = false; // stop trying to parse
        _pState = ParseState::IN_PARTIAL_FIXED_HEADER;
        return;
    }
    // we have enough data to parse the fixed header
    _fixedHeader = *( (FixedHeader*)_currentBinary.get_write() ); // just 4 bytes. Copy them
    _currentBinary.trim_front(sizeof(_fixedHeader)); // rewind the current binary

    // check if message is valid
    if (_fixedHeader.magic != txconstants::K2RPCMAGIC) {
        K2WARN("Received message with magic bit mismatch: " << _fixedHeader.magic << ", vs: " << txconstants::K2RPCMAGIC);
        _pState = ParseState::FAILED_STREAM;
        _parserFailureException = MagicMismatchException();
    }
    else {
        _pState = ParseState::WAIT_FOR_VARIABLE_HEADER; // onto getting the variable header
        K2DEBUG("wait_for_fixed_header: parsed");
    }
}

inline
void RPCParser::_stIN_PARTIAL_FIXED_HEADER() {
    // in this state, we come only after
    // 1. we had some data but not enough to parse the fixed header
    if (_currentBinary.size() == 0) {
        _shouldParse = false; // stop trying to parse
        K2DEBUG("partial_fixed_header: no new data yet");
        return; // no new data yet
    }

    // just copy the needed bytes from the binarys
    auto partSize = _partialBinary.size();
    auto curSize = _currentBinary.size();
    auto totalNeed = sizeof(_fixedHeader);

    K2DEBUG("partial_fixed_header: partSize=" << partSize << ", curSize=" << curSize << ", totalNeed=" << totalNeed);

    // 1. copy whatever was left in the partial binary
    std::memcpy((char*)&_fixedHeader, _partialBinary.get_write(), partSize);
    // done with the partial binary.
    std::move(_partialBinary).prefix(0);

    // check to make sure we have enough data in the incoming binary
    if (curSize < totalNeed - partSize) {
        K2WARN("Received continuation segment which doesn't have enough data: " << curSize
               << ", total: " << totalNeed <<", have: " << partSize);
        _pState = ParseState::FAILED_STREAM; // state machine will continue parsing and end up in failed state
        _parserFailureException = NonContinuationSegmentException();
        return;
    }

    // and copy the rest of what we need from the current binary
    std::memcpy((char*)&_fixedHeader + partSize, _currentBinary.get_write(), totalNeed - partSize);
    // rewind the current binary
    _currentBinary.trim_front(totalNeed - partSize);

    _pState = WAIT_FOR_VARIABLE_HEADER; // onto getting the variable header
    K2DEBUG("partial_fixed_header: parsed");
}

inline
void RPCParser::_stWAIT_FOR_VARIABLE_HEADER() {
    // set the feature vector so that we can use the API
    _metadata.features = _fixedHeader.features;

    // how many bytes we need off the wire
    size_t needBytes = _metadata.wireByteCount();
    size_t haveBytes = _currentBinary.size(); // NB we can only come in this method with no partial data

    K2DEBUG("wait_for_var_header: need=" << needBytes << ", have=" << haveBytes);
    // we come in this state only when we should try to get a variable header from the current binary
    if (needBytes > 0 && haveBytes == 0) {
        K2DEBUG("wait_for_var_header: no bytes in current segment. continuing");
        _shouldParse = false; // stop trying to parse
        return; // nothing to do - no new data, so remain in this state waiting for new data
    }
    else if (needBytes > haveBytes) {
        K2DEBUG("wait_for_var_header: need data but not enough present");
        // we have some new data, but not enough to parse the variable header. move it to the partial segment
        // and setup for state change to IN_PARTIAL_VARIABLE_HEADER
        _partialBinary = std::move(_currentBinary);
        _pState = ParseState::IN_PARTIAL_VARIABLE_HEADER;
        _shouldParse = false; // stop trying to parse
        return;
    }
    // if we came here, we either don't need any bytes, or we have all the bytes we need in _currentBinary

    // now for variable stuff
    if (_metadata.isPayloadSizeSet()) {
        std::memcpy((char*)&_metadata.payloadSize, _currentBinary.get_write(), sizeof(_metadata.payloadSize));
        K2DEBUG("wait_for_var_header: have payload size: " << _metadata.payloadSize);
        _currentBinary.trim_front(sizeof(_metadata.payloadSize));
    }
    if (_metadata.isRequestIDSet()) {
        std::memcpy((char*)&_metadata.requestID, _currentBinary.get_write(), sizeof(_metadata.requestID));
        K2DEBUG("wait_for_var_header: have request id: "<< _metadata.requestID);
        _currentBinary.trim_front(sizeof(_metadata.requestID));
    }
    if (_metadata.isResponseIDSet()) {
        std::memcpy((char*)&_metadata.responseID, _currentBinary.get_write(), sizeof(_metadata.responseID));
        K2DEBUG("wait_for_var_header: have response id: " << _metadata.responseID);
        _currentBinary.trim_front(sizeof(_metadata.responseID));
    }

    _pState = ParseState::WAIT_FOR_PAYLOAD; // onto getting the payload
    K2DEBUG("wait_for_var_header: parsed");
}

inline
void RPCParser::_stIN_PARTIAL_VARIABLE_HEADER() {
    // we are here because
    // 1. we need some bytes to determine message metadata
    // 2. there were not enough bytes in previous binary
    if (_currentBinary.size() == 0) {
        K2DEBUG("partial_var_header: no new data yet");
        _shouldParse = false; // stop trying to parse
        return; // no new data yet
    }
    // how many bytes we need off the wire
    auto partSize = _partialBinary.size();
    auto curSize = _currentBinary.size();
    auto totalNeed = _metadata.wireByteCount();
    K2DEBUG("partial_var_header: need=" << totalNeed <<", partsize=" << partSize << ", curSize=" << curSize);

    if (totalNeed > partSize + curSize) {
        K2WARN("Received partial variable header continuation segment which doesn't have enough data: " << curSize
               << ", total: " << totalNeed <<", have: " << partSize);
        _pState = ParseState::FAILED_STREAM; // state machine will keep going and end up in Failed state
        _parserFailureException = NonContinuationSegmentException();
        return;
    }

    // copy the bytes we need into a contiguious region.
    char _data[totalNeed];
    char* data = _data;

    std::memcpy(data, _partialBinary.get_write(), partSize);
    // done with the partial binary.
    std::move(_partialBinary).prefix(0);

    std::memcpy(data, _currentBinary.get_write(), totalNeed - partSize);
    // rewind the current binary
    _currentBinary.trim_front(totalNeed - partSize);

    // now set the variable fields
    if (_metadata.isPayloadSizeSet()) {
        std::memcpy((char*)&_metadata.payloadSize, data, sizeof(_metadata.payloadSize));
        K2DEBUG("partial_var_header: have payload size: " << _metadata.payloadSize);
        data += sizeof(_metadata.payloadSize);
    }
    if (_metadata.isRequestIDSet()) {
        std::memcpy((char*)&_metadata.requestID, data, sizeof(_metadata.requestID));
        K2DEBUG("partial_var_header: have request id: "<< _metadata.requestID);
        data += sizeof(_metadata.requestID);
    }
    if (_metadata.isResponseIDSet()) {
        std::memcpy((char*)&_metadata.responseID, data, sizeof(_metadata.responseID));
        K2DEBUG("partial_var_header: have response id: " << _metadata.responseID);
        data += sizeof(_metadata.responseID);
    }

    _pState = ParseState::WAIT_FOR_PAYLOAD; // onto getting the payload
    K2DEBUG("partial_var_header: parsed");
}

inline
void RPCParser::_stWAIT_FOR_PAYLOAD() {
    K2DEBUG("wait_for_payload");
    // check to see if we're expecting payload
    if (!_metadata.isPayloadSizeSet() ) {
        K2DEBUG("wait_for_payload: no payload expected");
        _pState = ParseState::READY_TO_DISPATCH; // ready to dispatch. State machine sill keep going and dispatch
        return;
    }
    if (!_payload) {
        // make a new payload to deliver. this payload won't support dynamic expansion (null allocator)
        _payload = std::make_unique<Payload>(nullptr, "");
    }
    auto available = _currentBinary.size();
    auto have = _payload->getSize();
    auto needed = _metadata.payloadSize - have;
    K2DEBUG("wait_for_payload: total=" << _metadata.payloadSize << ", have=" << have
            << ", needed=" << needed <<", available=" << available);

    // check to see if we're already set
    if (needed == 0) {
        K2DEBUG("wait_for_payload: we have the expected payload");
        _pState = ParseState::READY_TO_DISPATCH; // ready to dispatch. State machine will keep going and dispatch
        return;
    }
    if (available == 0) {
        _shouldParse = false; // got nothing left to parse. wait in this state for more data
        return;
    }

    // get whatever we can from the current binary. Let the state machine run again in this state
    // to determine if we had enough, or we need more
    auto bytesThisRound = std::min(needed, available);
    // last case, we have more data than we need. Extract a slice from the binary
    _payload->appendBinary(_currentBinary.share(0, bytesThisRound));
    // rewind the binary
    _currentBinary.trim_front(bytesThisRound);
    K2DEBUG("wait_for_payload: got payload from existing binary of size= " << bytesThisRound <<". remaining bytes=" << _currentBinary.size());
}

inline
void RPCParser::_stREADY_TO_DISPATCH() {
    K2DEBUG("ready_to_dispatch: cursize=" << _currentBinary.size());

    _messageObserver(_fixedHeader.verb, std::move(_metadata), std::move(_payload));

    // only now we're ready to process the next message
    _pState = RPCParser::ParseState::WAIT_FOR_FIXED_HEADER;
}

inline
void RPCParser::_stFAILED_STREAM() {
    K2WARN("Parsing of stream not possible");
    std::move(_partialBinary).prefix(0);
    std::move(_currentBinary).prefix(0);

    // release any partial data we have
    _shouldParse = false; // stop trying to parse
    _parserFailureObserver(std::make_exception_ptr(_parserFailureException));
}

inline
std::unique_ptr<Payload>
RPCParser::serializeMessage(Payload&& message, Verb verb, MessageMetadata metadata) {
    K2DEBUG("serializing message");
    auto userMessageSize = message.getSize();
    auto&& buffers = message.release();
    auto metaPayloadSize = metadata.isPayloadSizeSet() ? metadata.payloadSize : 0;

    if (userMessageSize == metaPayloadSize) {
        // the incoming message doesn't have room for a header
        buffers.insert(buffers.begin(), Binary(txconstants::MAX_HEADER_SIZE));
        userMessageSize += txconstants::MAX_HEADER_SIZE;
    }

    assert(userMessageSize == metaPayloadSize  + txconstants::MAX_HEADER_SIZE);
    auto headerSize = RPCParser::serializeHeader(buffers[0], verb, std::move(metadata));
    return std::make_unique<Payload>(std::move(buffers), headerSize + metaPayloadSize);
}

//
//  TODO: below given simple interface to do message reading and writting. Implementation are not efficient, since parser
//  currently is not based on streaming (PayloadWriter/PayloadReader) interfaces. We need to change parser to make it more
//  simple, so we can integrate it with MessageReader/Builder in more efficient way.
//

//
//  Simple message reader, which works as sink-source pattern for buffers-messages.
//  Customer of MessageReaders provides incoming data buffers using putBuffer function and collect
//  parsed messages using getMessage function.
//
class MessageReader
{
protected:
    std::queue<Binary> inBuffers;
    std::queue<MessageDescription> outMessages;
    RPCParser parser;
    Status status = Status::Ok;
public:
    MessageReader() : parser([]{return false;})
    {
        parser.registerMessageObserver([this](Verb verb, MessageMetadata metadata, std::unique_ptr<Payload> payload)
        {
            outMessages.emplace(verb, metadata, std::move(*payload));
        });

        parser.registerParserFailureObserver([this](std::exception_ptr)
        {
            status = Status::MessageParsingError;
        });
    }

    Status getStatus() { return status; }

    bool putBuffer(Binary binary)
    {
        if(status != Status::Ok)
            return false;

        inBuffers.push(std::move(binary));
        return true;
    }

    bool getMessage(MessageDescription& outMessage)
    {
        while(true)
        {
            if(!outMessages.empty())
            {
                outMessage = std::move(outMessages.front());
                outMessages.pop();
                return true;
            }

            if(status != Status::Ok || inBuffers.empty())
                return false;

            parser.feed(std::move(inBuffers.front()));
            inBuffers.pop();
            parser.dispatchSome();
        }

        return false;
    }

    Status readMessage(MessageDescription& outMessage, std::function<Status(Binary& outBuffer)> bufferSource)
    {
        while(true)
        {
            Binary buffer;
            RET_IF_BAD(bufferSource(buffer));

            putBuffer(std::move(buffer));
            if(getMessage(outMessage))
                return Status::Ok;

            RET_IF_BAD(status);
        }
    }

    static Status readSingleMessage(MessageDescription& outMessage, std::function<Status(Binary& outBuffer)> bufferSource)
    {
        MessageReader reader;
        return reader.readMessage(outMessage, std::move(bufferSource));
    }
};

//
//  Create messages to send
//
class MessageBuilder
{
protected:
    MessageDescription message;
    PayloadWriter::Position headerPosition;
    size_t headerSize;

    void defineHeaderSize()
    {
        writeHeader(txconstants::MAX_HEADER_SIZE);

        //  Truncate to real size
        PayloadWriter writer(message.payload, headerPosition);
        auto result = writer.skip(headerSize);
        K2ASSERT(result, "unable to skip");
        writer.truncateToCurrent();
    }

    void writeHeader(int maxBufferSize)
    {
        PayloadWriter writer(message.payload, headerPosition);

        void* headerMemory = nullptr;
        auto result = writer.reserveContiguousBuffer(maxBufferSize, headerMemory);
        K2ASSERT(result, "unable to reserve bytes");
        Binary headerBuffer = binaryReference(headerMemory, maxBufferSize);
        auto rcode = RPCParser::writeHeader(headerBuffer, message.verb, message.metadata, &headerSize);
        assert(rcode);
    }

    MessageBuilder(Verb verb, MessageMetadata metadata, Payload payload) : message(verb, metadata, std::move(payload)),
        headerPosition(message.payload.getWriter().getCurrent())
    {
        //  Write header just to estimate it's size and skip it
        message.metadata.setPayloadSize(1);  //  TODO: if we don't set size, RPCParser will not allocate enough space for it - we need to make this field static later
        defineHeaderSize();
    }

public:
    static MessageBuilder request(Verb verb, MessageMetadata metadata, Payload payload)
    {
        return MessageBuilder(verb, std::move(metadata), std::move(payload));
    }

    static MessageBuilder request(Verb verb, Payload payload)
    {
        return MessageBuilder(verb, MessageMetadata::newRequest(), std::move(payload));
    }

    static MessageBuilder request(Verb verb)
    {
        return MessageBuilder(verb, MessageMetadata::newRequest(), Payload());
    }

    static MessageBuilder response(Payload newMessagePayload, const MessageMetadata& originalRequestMessageMetadata)
    {
        return MessageBuilder(KnownVerbs::None, originalRequestMessageMetadata.createResponse(), std::move(newMessagePayload));
    }

    static MessageBuilder response(const MessageMetadata& originalRequestMessageMetadata)
    {
        return response(Payload(), originalRequestMessageMetadata);
    }

    static MessageBuilder response(Payload newMessagePayload, const MessageDescription& originalRequestMessage)
    {
        return response(std::move(newMessagePayload), originalRequestMessage.metadata);
    }

    PayloadWriter getWriter() { return message.payload.getWriter(); }

    template<typename... ArgsT>
    MessageBuilder& write(ArgsT&... args)
    {
        auto ret = getWriter().writeMany(args...);
        assert(ret);
        return* this;
    }

    Payload&& build()
    {
        int64_t payloadSize = (message.payload.getWriter().getCurrent() - headerPosition) - headerSize;
        assert(payloadSize > 0);
        message.metadata.payloadSize = payloadSize;
        writeHeader(headerSize);  //  Write header one more time now with updated size
        return std::move(message.payload);
    }
};

} // namespace k2
