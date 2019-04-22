//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
// stl
#include <cstdint> // for int types

// k2tx
#include "RPCTypes.h"
#include "Fragment.h"
#include "RPCHeader.h"
#include "Payload.h"
#include "Log.h"

namespace k2tx {

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
    // creates an RPC parser
    RPCParser();

    // destructor. Any incomplete messages are dropped
    ~RPCParser();

    // Utility method used to create a header for an outgoing message
    static Fragment SerializeHeader(Fragment fragment, Verb verb, MessageMetadata metadata);

    // This method should be called with the fragment in a stream of messages.
    // we handle messages which can span multiple fragments in this class.
    // The user should use the methods CanDispatch() to determine if it should call DispatchSome()
    // For performance reasons, you should only feed more data once all current data has been processed
    // this method will assert that it is not being called when CanDispatch() is true
    // see usage in TCPRPCChannel.cpp for example on how to setup a processing loop
    void Feed(Fragment fragment);

    // Use to determine if this parser could potentially dispatch some messages. It is possible that
    // in some cases no messages will be dispatched if DispatchSome() is called
    bool CanDispatch() { return _shouldParse;}

    // Ask the parser to process data in the incoming fragments and dispatch some messages. This method
    // dispatches 0 or more messages.
    // Under the covers, we consult seastar::need_preempt to stop dispatching even if we have more messages
    // so the user should keep calling DispatchSome until CanDispatch returns false
    void DispatchSome();

    // Call this method with a callback to observe incoming RPC messages
    void RegisterMessageObserver(MessageObserver_t messageObserver);

    // Call this method with a callback to observe parsing failure
    void RegisterParserFailureObserver(ParserFailureObserver_t parserFailureObserver);

private: // types
    enum ParseState: uint8_t {
        WAIT_FOR_FIXED_HEADER, // we're waiting for a header for a new message
        IN_PARTIAL_FIXED_HEADER, // we got parts of the fixed header but not all (fragmented fixed header)
        WAIT_FOR_VARIABLE_HEADER, // we got the fixed header, and now we need the variable fields
        IN_PARTIAL_VARIABLE_HEADER, // we got parts of the variable header but not all (fragmented variable header)
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

private: // fields
    // message observer
    MessageObserver_t _messageObserver;

    // parser failure observer
    ParserFailureObserver_t _parserFailureObserver;

    // this holds the exception which indicates the parser failure type
    std::exception _parserFailureException;

    // a flag we use to determine if we should try to parse for new messages
    bool _shouldParse;

    // the parser state
    ParseState _pState;

    // the fixed header for the current message
    FixedHeader _fixedHeader;

    // the metadata for the current message
    MessageMetadata _metadata;

    // the payload for the current message;
    std::unique_ptr<Payload> _payload;

    // partial fragment left over from previous parsing. Only used when header(variable or fixed) spans two fragments
    Fragment _partialFragment;

    // current incoming fragment
    Fragment _currentFragment;

private: // don't need
    RPCParser(const RPCParser& o) = delete;
    RPCParser(RPCParser&& o) = delete;
    RPCParser& operator=(const RPCParser& o) = delete;
    RPCParser& operator=(RPCParser&& o) = delete;
}; // class RPCParser

} // namespace k2tx
