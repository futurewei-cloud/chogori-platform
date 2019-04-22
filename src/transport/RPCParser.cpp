//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "RPCParser.h"
#include "Log.h"

namespace k2tx {
RPCParser::RPCParser():
    _shouldParse(false),
    _pState(ParseState::WAIT_FOR_FIXED_HEADER) {
    K2DEBUG("ctor");
    RegisterMessageObserver(nullptr);
    RegisterParserFailureObserver(nullptr);
}

RPCParser::~RPCParser(){
    K2DEBUG("dtor");
}

Fragment RPCParser::SerializeHeader(Fragment fragment, Verb verb, MessageMetadata meta) {
    K2DEBUG("serialize header");
    // take care of the fixed header first
    size_t writtenSoFar = 0;
    FixedHeader fHeader{.features=meta.features, .verb=verb};
    std::memcpy(fragment.get_write() + writtenSoFar, (Binary_t*)&fHeader, sizeof(fHeader));
    writtenSoFar += sizeof(fHeader);

    // now for variable stuff
    if (meta.IsPayloadSizeSet()) {
        K2DEBUG("have payload");
        std::memcpy(fragment.get_write() + writtenSoFar, (Binary_t*)&meta.payloadSize, sizeof(meta.payloadSize));
        writtenSoFar += sizeof(meta.payloadSize);
    }
    if (meta.IsRequestIDSet()) {
        K2DEBUG("have request id" << meta.requestID);
        std::memcpy(fragment.get_write() + writtenSoFar, (Binary_t*)&meta.requestID, sizeof(meta.requestID));
        writtenSoFar += sizeof(meta.requestID);
    }
    if (meta.IsResponseIDSet()) {
        K2DEBUG("have response id" << meta.responseID);
        std::memcpy(fragment.get_write() + writtenSoFar, (Binary_t*)&meta.responseID, sizeof(meta.responseID));
        writtenSoFar += sizeof(meta.responseID);
    }

    // all done. Truncate end of fragment
    K2DEBUG("wrote total bytes: " << writtenSoFar);
    fragment.trim(writtenSoFar);
    return std::move(fragment);
}

void RPCParser::RegisterMessageObserver(MessageObserver_t messageObserver) {
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

void RPCParser::RegisterParserFailureObserver(ParserFailureObserver_t parserFailureObserver) {
    K2DEBUG("register parser failure observer");
    if (parserFailureObserver == nullptr) {
        K2DEBUG("registering default parser failure observer");
        _parserFailureObserver = [](std::exception_ptr exc) {
            K2WARN("parser stream failure ocurred, but there is no observer registered: " << exc);
        };
    }
    else {
        K2DEBUG("registering parser failure observer");
        _parserFailureObserver = parserFailureObserver;
    }
}

void RPCParser::Feed(Fragment fragment) {
    K2DEBUG("feed bytes" << fragment.size());
    assert(!_shouldParse);

    // always move the incoming packet into the current fragment. If there was any partial data left from
    // previous parsing round, it would be in the _partialFragment fragment.
    _currentFragment = std::move(fragment);
    // we got some new data. We should be parsing it
    _shouldParse = true;
}

void RPCParser::DispatchSome() {
    K2DEBUG("dispatch some: " << _shouldParse);
    while(_shouldParse) {
        // parse and dispatch the next message
        _parseAndDispatchOne();

        if (seastar::need_preempt()) {
            K2DEBUG("we hogged the event loop enough");
            break;
        }
    }
}

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
                dispatched = true; // we should stop now
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

void RPCParser::_stWAIT_FOR_FIXED_HEADER() {
    // we come to this state when we think we have enough data to parse a new message from
    // the current fragment.
    K2DEBUG("wait_for_fixed_header");
    if (_currentFragment.size() == 0) {
        K2DEBUG("wait_for_fixed_header: empty fragment");
        return; // nothing to do - no new data, so remain in this state waiting for new data
    }

    if (_currentFragment.size() < sizeof(_fixedHeader)) {
        K2DEBUG("wait_for_fixed_header: not enough data");
        // we have some new data, but not enough to parse the fixed header. move it to the partial segment
        // and state change to IN_PARTIAL_FIXED_HEADER
        _partialFragment = std::move(_currentFragment);
        _pState = ParseState::IN_PARTIAL_FIXED_HEADER;
        return;
    }
    // we have enough data to parse the fixed header
    _fixedHeader = *( (FixedHeader*)_currentFragment.get_write() ); // just 4 bytes. Copy them
    _currentFragment.trim_front(sizeof(_fixedHeader)); // rewind the current fragment

    // check if message is valid
    if (_fixedHeader.magic != K2RPCMAGIC) {
        K2WARN("Received message with magic bit mismatch: " << _fixedHeader.magic << ", vs: " << K2RPCMAGIC);
        _pState = ParseState::FAILED_STREAM;
        _parserFailureException = MagicMismatchException();
    }
    else {
        _pState = ParseState::WAIT_FOR_VARIABLE_HEADER; // onto getting the variable header
        K2DEBUG("wait_for_fixed_header: parsed");
    }
}

void RPCParser::_stIN_PARTIAL_FIXED_HEADER() {
    // in this state, we come only after
    // 1. we had some data but not enough to parse the fixed header
    if (_currentFragment.size() == 0) {
        K2DEBUG("partial_fixed_header: no new data yet");
        return; // no new data yet
    }

    // just copy the needed bytes from the fragments
    auto partSize = _partialFragment.size();
    auto curSize = _currentFragment.size();
    auto totalNeed = sizeof(_fixedHeader);

    K2DEBUG("partial_fixed_header: partSize=" << partSize << ", curSize=" << curSize << ", totalNeed=" << totalNeed);

    // 1. copy whatever was left in the partial fragment
    std::memcpy((Binary_t*)&_fixedHeader, _partialFragment.get_write(), partSize);
    // done with the partial fragment.
    _partialFragment.release();

    // check to make sure we have enough data in the incoming fragment
    if (curSize < totalNeed - partSize) {
        K2WARN("Received continuation segment which doesn't have enough data: " << curSize
               << ", total: " << totalNeed <<", have: " << partSize);
        _pState = ParseState::FAILED_STREAM;
        _parserFailureException = NonContinuationSegmentException();
        return;
    }

    // and copy the rest of what we need from the current fragment
    std::memcpy((Binary_t*)&_fixedHeader + partSize, _currentFragment.get_write(), totalNeed - partSize);
    // rewind the current fragment
    _currentFragment.trim_front(totalNeed - partSize);

    _pState = WAIT_FOR_VARIABLE_HEADER; // onto getting the variable header
    K2DEBUG("partial_fixed_header: parsed");
}

void RPCParser::_stWAIT_FOR_VARIABLE_HEADER() {
    // set the feature vector so that we can use the API
    _metadata.features = _fixedHeader.features;

    // how many bytes we need off the wire
    size_t needBytes = _metadata.WireByteCount();
    size_t haveBytes = _currentFragment.size();

    K2DEBUG("wait_for_var_header: need=" << needBytes << ", have=" << haveBytes);
    // we come in this state only when we should try to get a variable header from the current fragment
    if (needBytes > 0 && haveBytes == 0) {
        K2DEBUG("wait_for_var_header: no bytes in current segment. continuing");
        return; // nothing to do - no new data, so remain in this state waiting for new data
    }
    else if (needBytes > haveBytes) {
        K2DEBUG("wait_for_var_header: need data but not enough present");
        // we have some new data, but not enough to parse the variable header. move it to the partial segment
        // and state change to IN_PARTIAL_VARIABLE_HEADER
        _partialFragment = std::move(_currentFragment);
        _pState = ParseState::IN_PARTIAL_VARIABLE_HEADER;
        return;
    }
    // if we came here, we either don't need any bytes, or we have all the bytes we need in _currentFragment

    // now for variable stuff
    if (_metadata.IsPayloadSizeSet()) {
        std::memcpy((Binary_t*)&_metadata.payloadSize, _currentFragment.get_write(), sizeof(_metadata.payloadSize));
        K2DEBUG("wait_for_var_header: have payload size: " << _metadata.payloadSize);
        _currentFragment.trim_front(sizeof(_metadata.payloadSize));
    }
    if (_metadata.IsRequestIDSet()) {
        std::memcpy((Binary_t*)&_metadata.requestID, _currentFragment.get_write(), sizeof(_metadata.requestID));
        K2DEBUG("wait_for_var_header: have request id: "<< _metadata.requestID);
        _currentFragment.trim_front(sizeof(_metadata.requestID));
    }
    if (_metadata.IsResponseIDSet()) {
        std::memcpy((Binary_t*)&_metadata.responseID, _currentFragment.get_write(), sizeof(_metadata.responseID));
        K2DEBUG("wait_for_var_header: have response id: " << _metadata.responseID);
        _currentFragment.trim_front(sizeof(_metadata.responseID));
    }

    _pState = ParseState::WAIT_FOR_PAYLOAD; // onto getting the payload
    K2DEBUG("wait_for_var_header: parsed");
}

void RPCParser::_stIN_PARTIAL_VARIABLE_HEADER() {
    // we are here because
    // 1. we need some bytes to determine message metadata
    // 2. there were not enough bytes in previous fragment
    if (_currentFragment.size() == 0) {
        K2DEBUG("partial_var_header: no new data yet");
        return; // no new data yet
    }
    // how many bytes we need off the wire
    auto partSize = _partialFragment.size();
    auto curSize = _currentFragment.size();
    auto totalNeed = _metadata.WireByteCount();
    K2DEBUG("partial_var_header: need=" << totalNeed <<", partsize=" << partSize << ", curSize=" << curSize);

    if (totalNeed > partSize + curSize) {
        K2WARN("Received partial variable header continuation segment which doesn't have enough data: " << curSize
               << ", total: " << totalNeed <<", have: " << partSize);
        _pState = ParseState::FAILED_STREAM;
        _parserFailureException = NonContinuationSegmentException();
        return;
    }

    // copy the bytes we need into a contiguious region.
    Binary_t _data[totalNeed];
    Binary_t* data = _data;

    std::memcpy(data, _partialFragment.get_write(), partSize);
    // done with the partial fragment.
    _partialFragment.release();

    std::memcpy(data, _currentFragment.get_write(), totalNeed - partSize);
    // rewind the current fragment
    _currentFragment.trim_front(totalNeed - partSize);

    // now set the variable fields
    if (_metadata.IsPayloadSizeSet()) {
        std::memcpy((Binary_t*)&_metadata.payloadSize, data, sizeof(_metadata.payloadSize));
        K2DEBUG("partial_var_header: have payload size: " << _metadata.payloadSize);
        data += sizeof(_metadata.payloadSize);
    }
    if (_metadata.IsRequestIDSet()) {
        std::memcpy((Binary_t*)&_metadata.requestID, data, sizeof(_metadata.requestID));
        K2DEBUG("partial_var_header: have request id: "<< _metadata.requestID);
        data += sizeof(_metadata.requestID);
    }
    if (_metadata.IsResponseIDSet()) {
        std::memcpy((Binary_t*)&_metadata.responseID, data, sizeof(_metadata.responseID));
        K2DEBUG("partial_var_header: have response id: " << _metadata.responseID);
        data += sizeof(_metadata.responseID);
    }

    _pState = ParseState::WAIT_FOR_PAYLOAD; // onto getting the payload
    K2DEBUG("partial_var_header: parsed");
}

void RPCParser::_stWAIT_FOR_PAYLOAD() {
    K2DEBUG("wait_for_payload");
    // check to see if we're expecting payload
    if (!_metadata.IsPayloadSizeSet() ) {
        K2DEBUG("wait_for_payload: no payload expected");
        _pState = ParseState::READY_TO_DISPATCH; // ready to dispatch
        return;
    }
    if (!_payload) {
        // this payload doesn't support dynamic expansion (null allocator)
        _payload = std::make_unique<Payload>(nullptr, "");
    }
    auto available = _currentFragment.size();
    auto have = _payload->Size();
    auto needed = _metadata.payloadSize - have;
    K2DEBUG("wait_for_payload: total=" << _metadata.payloadSize << ", have=" << have
            << ", needed=" << needed <<", available=" << available);

    // check to see if we're already set
    if (_metadata.payloadSize == have) {
        K2DEBUG("wait_for_payload: we have the expected payload");
        _pState = ParseState::READY_TO_DISPATCH; // ready to dispatch
        return;
    }

    // process the case where we have some data which is exactly what we need, or not enough
    if (needed > available) {
        K2DEBUG("wait_for_payload: consumed available but need more");
        _payload->AppendFragment(std::move(_currentFragment));
        return;
    }

    // last case, we have more data than we need. Extract a slice from the fragment
    _payload->AppendFragment(_currentFragment.share(0, needed));
    // rewind the fragment
    _currentFragment.trim_front(needed);
    _pState = ParseState::READY_TO_DISPATCH; // ready to dispatch
    K2DEBUG("wait_for_payload: got payload from existing fragment. remaining bytes: " << _currentFragment.size());
}

void RPCParser::_stREADY_TO_DISPATCH() {
    K2DEBUG("ready_to_dispatch: cursize=" << _currentFragment.size());
    _messageObserver(_fixedHeader.verb, std::move(_metadata), std::move(_payload));

    // only now we're ready to process the next message
    _pState = RPCParser::ParseState::WAIT_FOR_FIXED_HEADER;

    // only try to parse more if we have any data left in current fragment
    _shouldParse = (_currentFragment.size() != 0);
}

void RPCParser::_stFAILED_STREAM() {
    K2WARN("Parsing of stream not possible");
    _shouldParse = false; // no more parsing is possible
    _parserFailureObserver(std::make_exception_ptr(_parserFailureException));
}

} // k2tx
