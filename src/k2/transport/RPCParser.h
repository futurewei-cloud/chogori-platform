/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

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

    // indicates that checksum validation has failed
    class ChecksumValidationException : public std::exception {};

    // indicates that we expected to receive the second segment for partial header, but
    // the segment we received did not have enough data.
    class NonContinuationSegmentException : public std::exception {};

   public:
    // creates an RPC parser with the given preemptor function. Users can request that we validate/generate checksums
    // at the expense of extra read pass over the data
    RPCParser(std::function<bool()> preemptor, bool useChecksum);

    // destructor. Any incomplete messages are dropped
    ~RPCParser();

    // Utility method used to create a header for an outgoing message
    // the incoming binary must have exactly txconstants::MAX_HEADER_SIZE bytes
    // reserved in the beginning.
    // the incoming binary is is populated with the header and trimmed from the front to the first
    // byte of the header
    // returns the number of bytes written as header
    static size_t serializeHeader(Binary& binary, Verb verb, MessageMetadata metadata);

    //
    //  Write transport header to the binary. Return false, if not enough space in binary.
    //
    static bool writeHeader(Binary& binary, Verb verb, MessageMetadata meta);

    // Utility method used to serialize the given user message into a transport message, expressed as a Payload.
    // The user can also provide features via the metadata field
    static std::unique_ptr<Payload> serializeMessage(Payload&& message, Verb verb, MessageMetadata metadata);

    // This method is used to prepare a given mesage for sending. The resulting iovec can be passed to lower-level
    // transport as packets to send.
    std::vector<Binary> prepareForSend(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata);

    // This method should be called with the binary in a stream of messages.
    // we handle messages which can span multiple binaries in this class.
    // The user should use the methods CanDispatch() to determine if it should call DispatchSome()
    // For performance reasons, you should only feed more data once all current data has been processed
    // this method will assert that it is not being called when CanDispatch() is true
    // see usage in TCPRPCChannel.cpp for example on how to setup a processing loop
    void feed(Binary&& binary);

    // Use to determine if this parser could potentially dispatch some messages. It is possible that
    // in some cases no messages will be dispatched if DispatchSome() is called
    bool canDispatch();

    // Ask the parser to process data in the incoming binarys and dispatch some messages. This method
    // dispatches 0 or more messages.
    // Under the covers, we consult the preemptor function to stop dispatching even if we have more messages
    // so the user should keep calling DispatchSome until CanDispatch returns false
    void dispatchSome();

    // Call this method with a callback to observe incoming RPC messages
    void registerMessageObserver(MessageObserver_t messageObserver);

    // Call this method with a callback to observe parsing failure
    void registerParserFailureObserver(ParserFailureObserver_t parserFailureObserver);

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
    void _parseAndDispatchOne();

    // state machine handlers
    void _stWAIT_FOR_FIXED_HEADER();
    void _stIN_PARTIAL_FIXED_HEADER();
    void _stWAIT_FOR_VARIABLE_HEADER();
    void _stIN_PARTIAL_VARIABLE_HEADER();
    void _stWAIT_FOR_PAYLOAD();
    void _stREADY_TO_DISPATCH();
    void _stFAILED_STREAM();

    void _setParserFailure(std::exception&& exc);

    static bool append(Binary& binary, size_t& writeOffset, const void* data, size_t size);

    template <typename T>
    static bool appendRaw(Binary& binary, size_t& writeOffset, const T& data) {
        return append(binary, writeOffset, &data, sizeof(T));
    }

   private:  // fields
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

    // flag used to determine if we should compute/validate checksums
    bool _useChecksum;

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
} // namespace k2
