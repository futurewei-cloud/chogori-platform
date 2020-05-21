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

#include "RPCParser.h"
namespace k2 {

bool RPCParser::append(Binary& binary, size_t& writeOffset, const void* data, size_t size) {
    if (binary.size() < writeOffset + size)
        return false;
    std::memcpy(binary.get_write() + writeOffset, data, size);
    writeOffset += size;
    return true;
}

bool RPCParser::canDispatch() { return _shouldParse; }

void RPCParser::_setParserFailure(std::exception&& exc) {
    _pState = ParseState::FAILED_STREAM;
    _parserFailureException = std::move(exc);
}

RPCParser::RPCParser(std::function<bool()> preemptor, bool useChecksum) :
        _shouldParse(false),
        _useChecksum(useChecksum),
        _pState(ParseState::WAIT_FOR_FIXED_HEADER),
        _preemptor(preemptor) {
    K2DEBUG("ctor");
    registerMessageObserver(nullptr);
    registerParserFailureObserver(nullptr);
}

RPCParser::~RPCParser() {
    K2DEBUG("dtor");
}

size_t RPCParser::serializeHeader(Binary& binary, Verb verb, MessageMetadata meta) {
    // we need to write a header of this many bytes:
    auto headerSize = sizeof(FixedHeader) + meta.wireByteCount();
    assert(txconstants::MAX_HEADER_SIZE >= headerSize);     // make sure our headers haven't gotten too big
    assert(binary.size() >= txconstants::MAX_HEADER_SIZE);  // make sure we have the room

    K2DEBUG("serialize header. Need bytes: " << headerSize);
    // trim the header binary so that it starts at the first header byte
    binary.trim_front(txconstants::MAX_HEADER_SIZE - headerSize);

    auto rcode = writeHeader(binary, verb, meta);
    assert(rcode);

    return headerSize;
}

//
//  Write header of the
//
bool RPCParser::writeHeader(Binary& binary, Verb verb, MessageMetadata meta) {
    size_t writeOffset = 0;

    // take care of the fixed header first
    FixedHeader fHeader;
    fHeader.features = meta.features;
    fHeader.verb = verb;

    if (!appendRaw(binary, writeOffset, fHeader))
        return false;

    // now for variable stuff
    if (meta.isPayloadSizeSet()) {
        K2DEBUG("have payload=" << meta.payloadSize);
        if (!appendRaw(binary, writeOffset, meta.payloadSize))
            return false;
    }
    if (meta.isRequestIDSet()) {
        K2DEBUG("have request id=" << meta.requestID);
        if (!appendRaw(binary, writeOffset, meta.requestID))
            return false;
    }
    if (meta.isResponseIDSet()) {
        K2DEBUG("have response id=" << meta.responseID);
        if (!appendRaw(binary, writeOffset, meta.responseID))
            return false;
    }
    if (meta.isChecksumSet()) {
        K2DEBUG("have checksum=" << meta.checksum);
        if (!appendRaw(binary, writeOffset, meta.checksum))
            return false;
    }
    // all done.
    K2DEBUG("Write offset after writing header: " << writeOffset);

    return true;
}

void RPCParser::registerMessageObserver(MessageObserver_t messageObserver) {
    K2DEBUG("register message observer");
    if (messageObserver == nullptr) {
        K2DEBUG("registering default observer");
        _messageObserver = [](Verb verb, MessageMetadata, std::unique_ptr<Payload>) {
            K2WARN("Dropping message: " << verb << " since there is no observer registered");
        };
    } else {
        K2DEBUG("registering observer");
        _messageObserver = messageObserver;
    }
}

void RPCParser::registerParserFailureObserver(ParserFailureObserver_t parserFailureObserver) {
    K2DEBUG("register parser failure observer");
    if (parserFailureObserver == nullptr) {
        K2DEBUG("registering default parser failure observer");
        _parserFailureObserver = [](std::exception_ptr) {
            K2WARN("parser stream failure ocurred, but there is no observer registered");
        };
    } else {
        K2DEBUG("registering parser failure observer");
        _parserFailureObserver = parserFailureObserver;
    }
}

void RPCParser::feed(Binary&& binary) {
    K2DEBUG("feed bytes" << binary.size());
    assert(_currentBinary.empty());

    // always move the incoming packet into the current binary. If there was any partial data left from
    // previous parsing round, it would be in the _partialBinary binary.
    _currentBinary = std::move(binary);
    _shouldParse = true;  // signal the parser that we should continue/start parsing data
}

void RPCParser::dispatchSome() {
    K2DEBUG("dispatch some: " << canDispatch());
    while (canDispatch()) {
        // parse and dispatch the next message
        _parseAndDispatchOne();

        if (_preemptor && _preemptor()) {
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
    while (!dispatched && _shouldParse) {
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

void RPCParser::_stWAIT_FOR_FIXED_HEADER() {
    // we come to this state when we think we have enough data to parse a new message from
    // the current binary.
    _payload.reset();  // get rid of any previous payload

    K2DEBUG("wait_for_fixed_header");
    if (_currentBinary.size() == 0) {
        K2DEBUG("wait_for_fixed_header: empty binary");
        _shouldParse = false;  // stop trying to parse
        return;                // nothing to do - no new data, so remain in this state waiting for new data
    }

    if (_currentBinary.size() < sizeof(_fixedHeader)) {
        K2DEBUG("wait_for_fixed_header: not enough data");
        // we have some new data, but not enough to parse the fixed header. move it to the partial segment
        // and setup for state change to IN_PARTIAL_FIXED_HEADER
        _partialBinary = std::move(_currentBinary);
        _shouldParse = false;  // stop trying to parse
        _pState = ParseState::IN_PARTIAL_FIXED_HEADER;
        return;
    }
    // we have enough data to parse the fixed header
    _fixedHeader = *((FixedHeader*)_currentBinary.get_write());  // just 4 bytes. Copy them
    _currentBinary.trim_front(sizeof(_fixedHeader));             // rewind the current binary

    // check if message is valid
    if (_fixedHeader.magic != txconstants::K2RPCMAGIC) {
        K2WARN("Received message with magic bit mismatch: " << int(_fixedHeader.magic) << ", vs: " << int(txconstants::K2RPCMAGIC));
        _setParserFailure(MagicMismatchException());
        return;
    }
    _pState = ParseState::WAIT_FOR_VARIABLE_HEADER;  // onto getting the variable header
    K2DEBUG("wait_for_fixed_header: parsed");
}

void RPCParser::_stIN_PARTIAL_FIXED_HEADER() {
    // in this state, we come only after
    // 1. we had some data but not enough to parse the fixed header
    if (_currentBinary.size() == 0) {
        _shouldParse = false;  // stop trying to parse
        K2DEBUG("partial_fixed_header: no new data yet");
        return;  // no new data yet
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
                                                                                << ", total: " << totalNeed << ", have: " << partSize);
        _setParserFailure(NonContinuationSegmentException());
        return;
    }

    // and copy the rest of what we need from the current binary
    std::memcpy((char*)&_fixedHeader + partSize, _currentBinary.get_write(), totalNeed - partSize);
    // rewind the current binary
    _currentBinary.trim_front(totalNeed - partSize);

    _pState = WAIT_FOR_VARIABLE_HEADER;  // onto getting the variable header
    K2DEBUG("partial_fixed_header: parsed");
}

void RPCParser::_stWAIT_FOR_VARIABLE_HEADER() {
    // set the feature vector so that we can use the API
    _metadata.features = _fixedHeader.features;

    // how many bytes we need off the wire
    size_t needBytes = _metadata.wireByteCount();
    size_t haveBytes = _currentBinary.size();  // NB we can only come in this method with no partial data

    K2DEBUG("wait_for_var_header: need=" << needBytes << ", have=" << haveBytes);
    // we come in this state only when we should try to get a variable header from the current binary
    if (needBytes > 0 && haveBytes == 0) {
        K2DEBUG("wait_for_var_header: no bytes in current segment. continuing");
        _shouldParse = false;  // stop trying to parse
        return;                // nothing to do - no new data, so remain in this state waiting for new data
    } else if (needBytes > haveBytes) {
        K2DEBUG("wait_for_var_header: need data but not enough present");
        // we have some new data, but not enough to parse the variable header. move it to the partial segment
        // and setup for state change to IN_PARTIAL_VARIABLE_HEADER
        _partialBinary = std::move(_currentBinary);
        _pState = ParseState::IN_PARTIAL_VARIABLE_HEADER;
        _shouldParse = false;  // stop trying to parse
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
        K2DEBUG("wait_for_var_header: have request id: " << _metadata.requestID);
        _currentBinary.trim_front(sizeof(_metadata.requestID));
    }
    if (_metadata.isResponseIDSet()) {
        std::memcpy((char*)&_metadata.responseID, _currentBinary.get_write(), sizeof(_metadata.responseID));
        K2DEBUG("wait_for_var_header: have response id: " << _metadata.responseID);
        _currentBinary.trim_front(sizeof(_metadata.responseID));
    }
    if (_metadata.isChecksumSet()) {
        std::memcpy((char*)&_metadata.checksum, _currentBinary.get_write(), sizeof(_metadata.checksum));
        K2DEBUG("wait_for_var_header: have checksum: " << _metadata.checksum);
        _currentBinary.trim_front(sizeof(_metadata.checksum));
    }
    _pState = ParseState::WAIT_FOR_PAYLOAD;  // onto getting the payload
    K2DEBUG("wait_for_var_header: parsed");
}

void RPCParser::_stIN_PARTIAL_VARIABLE_HEADER() {
    // we are here because
    // 1. we need some bytes to determine message metadata
    // 2. there were not enough bytes in previous binary
    if (_currentBinary.size() == 0) {
        K2DEBUG("partial_var_header: no new data yet");
        _shouldParse = false;  // stop trying to parse
        return;                // no new data yet
    }
    // how many bytes we need off the wire
    auto partSize = _partialBinary.size();
    auto curSize = _currentBinary.size();
    auto totalNeed = _metadata.wireByteCount();
    K2DEBUG("partial_var_header: need=" << totalNeed << ", partsize=" << partSize << ", curSize=" << curSize);

    if (totalNeed > partSize + curSize) {
        K2WARN("Received partial variable header continuation segment which doesn't have enough data: "
                << curSize << ", total: " << totalNeed << ", have: " << partSize);
        _setParserFailure(NonContinuationSegmentException());
        return;
    }
    K2ASSERT(totalNeed <= txconstants::MAX_HEADER_SIZE, "invalid needed bytes determined: " << totalNeed);
    // copy the bytes we need into a contiguous region.
    char _data[txconstants::MAX_HEADER_SIZE];
    char* data = _data;

    std::memcpy(data, _partialBinary.get_write(), partSize);
    // done with the partial binary.
    std::move(_partialBinary).prefix(0);

    std::memcpy(data + partSize, _currentBinary.get_write(), totalNeed - partSize);
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
        K2DEBUG("partial_var_header: have request id: " << _metadata.requestID);
        data += sizeof(_metadata.requestID);
    }
    if (_metadata.isResponseIDSet()) {
        std::memcpy((char*)&_metadata.responseID, data, sizeof(_metadata.responseID));
        K2DEBUG("partial_var_header: have response id: " << _metadata.responseID);
        data += sizeof(_metadata.responseID);
    }
    if (_metadata.isChecksumSet()) {
        std::memcpy((char*)&_metadata.checksum, data, sizeof(_metadata.checksum));
        K2DEBUG("partial_var_header: have checksum: " << _metadata.checksum);
        data += sizeof(_metadata.checksum);
    }
    _pState = ParseState::WAIT_FOR_PAYLOAD;  // onto getting the payload
    K2DEBUG("partial_var_header: parsed");
}

void RPCParser::_stWAIT_FOR_PAYLOAD() {
    K2DEBUG("wait_for_payload");
    // check to see if we're expecting payload
    if (!_metadata.isPayloadSizeSet()) {
        K2DEBUG("wait_for_payload: no payload expected");
        _pState = ParseState::READY_TO_DISPATCH;  // ready to dispatch. State machine sill keep going and dispatch
        return;
    }
    if (!_payload) {
        // make a new payload to deliver. this payload won't support dynamic expansion (null allocator)
        _payload = std::make_unique<Payload>();
    }
    auto available = _currentBinary.size();
    auto have = _payload->getSize();
    auto needed = _metadata.payloadSize - have;
    K2DEBUG("wait_for_payload: total=" << _metadata.payloadSize << ", have=" << have
                                       << ", needed=" << needed << ", available=" << available);

    // check to see if we're already set
    if (needed == 0) {
        K2DEBUG("wait_for_payload: we have the expected payload");
        _pState = ParseState::READY_TO_DISPATCH;  // ready to dispatch. State machine will keep going and dispatch
        return;
    }
    if (available == 0) {
        _shouldParse = false;  // got nothing left to parse. wait in this state for more data
        return;
    }

    // get whatever we can from the current binary. Let the state machine run again in this state
    // to determine if we had enough, or we need more
    auto bytesThisRound = std::min(needed, available);
    // last case, we have more data than we need. Extract a slice from the binary
    _payload->appendBinary(_currentBinary.share(0, bytesThisRound));
    // rewind the binary
    _currentBinary.trim_front(bytesThisRound);
    K2DEBUG("wait_for_payload: got payload from existing binary of size= " << bytesThisRound << ". remaining bytes=" << _currentBinary.size());
}

void RPCParser::_stREADY_TO_DISPATCH() {
    K2DEBUG("ready_to_dispatch: cursize=" << _currentBinary.size());
    if (_useChecksum && _payload) {
        if (!_metadata.isChecksumSet()) {
            K2DEBUG("metadata doesn't have crc checksum");
            _setParserFailure(ChecksumValidationException());
            return;
        }
        auto checksum = _payload->computeCrc32c();
        if (checksum != _metadata.checksum) {
            K2DEBUG("checksum doesn't match: have=" << _metadata.checksum << ", received=" << checksum);
            _setParserFailure(ChecksumValidationException());
            return;
        }
    }
    _messageObserver(_fixedHeader.verb, std::move(_metadata), std::move(_payload));

    // only now we're ready to process the next message
    _pState = RPCParser::ParseState::WAIT_FOR_FIXED_HEADER;
}

void RPCParser::_stFAILED_STREAM() {
    K2WARN("Parsing of stream not possible");
    std::move(_partialBinary).prefix(0);
    std::move(_currentBinary).prefix(0);

    // release any partial data we have
    _shouldParse = false;  // stop trying to parse
    _parserFailureObserver(std::make_exception_ptr(_parserFailureException));
}

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

    assert(userMessageSize == metaPayloadSize + txconstants::MAX_HEADER_SIZE);
    auto headerSize = RPCParser::serializeHeader(buffers[0], verb, std::move(metadata));
    return std::make_unique<Payload>(std::move(buffers), headerSize + metaPayloadSize);
}

std::vector<Binary>
RPCParser::prepareForSend(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata) {
    assert(payload->getSize() >= txconstants::MAX_HEADER_SIZE);
    auto dataSize = payload->getSize() - txconstants::MAX_HEADER_SIZE;
    metadata.setPayloadSize(dataSize);
    K2DEBUG("send: verb=" << int(verb) << ", payloadSize=" << dataSize);
    if (_useChecksum) {
        // compute checksum starting at MAX_HEADER_SIZE until end of payload
        payload->seek(txconstants::MAX_HEADER_SIZE);
        auto checksum = payload->computeCrc32c();
        K2DEBUG("Sending with checksum=" << checksum);
        metadata.setChecksum(checksum);
    }
    // disassemble the payload so that we can write the header in the first binary
    auto&& buffers = payload->release();
    // write the header into the headroom of the first binary and remember to send the extra bytes
    dataSize += RPCParser::serializeHeader(buffers[0], verb, std::move(metadata));
    // write out the header and data
    K2DEBUG("writing message: verb=" << int(verb) << ", messageSize=" << dataSize);
    size_t bufIdx = 0;
    while (bufIdx < buffers.size() && dataSize > 0) {
        auto& buf = buffers[bufIdx];
        if (buf.size() > dataSize) {
            buf.trim(dataSize);
        }
        dataSize -= buf.size();
        ++bufIdx;
    }
    buffers.resize(std::min(buffers.size(), bufIdx));  // remove any unneeded binaries
    return std::move(buffers);
}

} // namespace
