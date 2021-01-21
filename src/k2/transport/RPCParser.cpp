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
    registerMessageObserver(nullptr);
    registerParserFailureObserver(nullptr);
}

RPCParser::~RPCParser() {
}

size_t RPCParser::serializeHeader(Binary& binary, Verb verb, MessageMetadata meta) {
    // we need to write a header of this many bytes:
    auto headerSize = sizeof(FixedHeader) + meta.wireByteCount();
    K2ASSERT(log::tx, txconstants::MAX_HEADER_SIZE >= headerSize, "header size too big");     // make sure our headers haven't gotten too big
    K2ASSERT(log::tx, binary.size() >= txconstants::MAX_HEADER_SIZE, "no room in binary");  // make sure we have the room

    // trim the header binary so that it starts at the first header byte
    binary.trim_front(txconstants::MAX_HEADER_SIZE - headerSize);

    auto rcode = writeHeader(binary, verb, meta);
    K2ASSERT(log::tx, rcode, "unable to write header");

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
        if (!appendRaw(binary, writeOffset, meta.payloadSize))
            return false;
    }
    if (meta.isRequestIDSet()) {
        if (!appendRaw(binary, writeOffset, meta.requestID))
            return false;
    }
    if (meta.isResponseIDSet()) {
        if (!appendRaw(binary, writeOffset, meta.responseID))
            return false;
    }
    if (meta.isChecksumSet()) {
        if (!appendRaw(binary, writeOffset, meta.checksum))
            return false;
    }
    // all done.

    return true;
}

void RPCParser::registerMessageObserver(MessageObserver_t messageObserver) {
    if (messageObserver == nullptr) {
        _messageObserver = [](Verb verb, MessageMetadata, std::unique_ptr<Payload>) {
            K2LOG_W(log::tx, "Dropping message: verb={} since there is no registered observer", int(verb));
        };
    } else {
        _messageObserver = messageObserver;
    }
}

void RPCParser::registerParserFailureObserver(ParserFailureObserver_t parserFailureObserver) {
    if (parserFailureObserver == nullptr) {
        _parserFailureObserver = [](std::exception_ptr) {
            K2LOG_W(log::tx, "parser stream failure ocurred, but there is no observer registered");
        };
    } else {
        _parserFailureObserver = parserFailureObserver;
    }
}

void RPCParser::feed(Binary&& binary) {
    // always move the incoming packet into the current binary. If there was any partial data left from
    // previous parsing round, it would be in the _partialBinary binary.
    _currentBinary = std::move(binary);
    _shouldParse = true;  // signal the parser that we should continue/start parsing data
}

void RPCParser::dispatchSome() {
    while (canDispatch()) {
        // parse and dispatch the next message
        _parseAndDispatchOne();

        if (_preemptor && _preemptor()) {
            break;
        }
    }
}

void RPCParser::_parseAndDispatchOne() {
    // keep going through the motions while we can still keep parsing data
    // or we've dispatched a message
    bool dispatched = false;
    while (!dispatched && _shouldParse) {
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
                K2ASSERT(log::tx, false, "Unknown parser state");
                break;
            }
        }
    }
}

void RPCParser::_stWAIT_FOR_FIXED_HEADER() {
    // we come to this state when we think we have enough data to parse a new message from
    // the current binary.
    _payload.reset();  // get rid of any previous payload

    if (_currentBinary.size() == 0) {
        _shouldParse = false;  // stop trying to parse
        return;                // nothing to do - no new data, so remain in this state waiting for new data
    }

    if (_currentBinary.size() < sizeof(_fixedHeader)) {
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
        K2LOG_W(log::tx, "Received message with magic bit mismatch: {} vs {}", int(_fixedHeader.magic), int(txconstants::K2RPCMAGIC));
        _setParserFailure(MagicMismatchException());
        return;
    }
    _pState = ParseState::WAIT_FOR_VARIABLE_HEADER;  // onto getting the variable header
    K2LOG_V(log::tx, "wait_for_fixed_header: parsed");
}

void RPCParser::_stIN_PARTIAL_FIXED_HEADER() {
    // in this state, we come only after
    // 1. we had some data but not enough to parse the fixed header
    if (_currentBinary.size() == 0) {
        _shouldParse = false;  // stop trying to parse
        return;  // no new data yet
    }

    // just copy the needed bytes from the binarys
    auto partSize = _partialBinary.size();
    auto curSize = _currentBinary.size();
    auto totalNeed = sizeof(_fixedHeader);

    // 1. copy whatever was left in the partial binary
    std::memcpy((char*)&_fixedHeader, _partialBinary.get_write(), partSize);
    // done with the partial binary.
    std::move(_partialBinary).prefix(0);

    // check to make sure we have enough data in the incoming binary
    if (curSize < totalNeed - partSize) {
        K2LOG_W(log::tx, "Received continuation segment which doesn't have enough data: {}, total: {}, have: {}",
            curSize, totalNeed, partSize);
        _setParserFailure(NonContinuationSegmentException());
        return;
    }

    // and copy the rest of what we need from the current binary
    std::memcpy((char*)&_fixedHeader + partSize, _currentBinary.get_write(), totalNeed - partSize);
    // rewind the current binary
    _currentBinary.trim_front(totalNeed - partSize);

    _pState = WAIT_FOR_VARIABLE_HEADER;  // onto getting the variable header
}

void RPCParser::_stWAIT_FOR_VARIABLE_HEADER() {
    // set the feature vector so that we can use the API
    _metadata.features = _fixedHeader.features;

    // how many bytes we need off the wire
    size_t needBytes = _metadata.wireByteCount();
    size_t haveBytes = _currentBinary.size();  // NB we can only come in this method with no partial data

    // we come in this state only when we should try to get a variable header from the current binary
    if (needBytes > 0 && haveBytes == 0) {
        _shouldParse = false;  // stop trying to parse
        return;                // nothing to do - no new data, so remain in this state waiting for new data
    } else if (needBytes > haveBytes) {
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
        _currentBinary.trim_front(sizeof(_metadata.payloadSize));
    }
    if (_metadata.isRequestIDSet()) {
        std::memcpy((char*)&_metadata.requestID, _currentBinary.get_write(), sizeof(_metadata.requestID));
        _currentBinary.trim_front(sizeof(_metadata.requestID));
    }
    if (_metadata.isResponseIDSet()) {
        std::memcpy((char*)&_metadata.responseID, _currentBinary.get_write(), sizeof(_metadata.responseID));
        _currentBinary.trim_front(sizeof(_metadata.responseID));
    }
    if (_metadata.isChecksumSet()) {
        std::memcpy((char*)&_metadata.checksum, _currentBinary.get_write(), sizeof(_metadata.checksum));
        _currentBinary.trim_front(sizeof(_metadata.checksum));
    }
    _pState = ParseState::WAIT_FOR_PAYLOAD;  // onto getting the payload
}

void RPCParser::_stIN_PARTIAL_VARIABLE_HEADER() {
    // we are here because
    // 1. we need some bytes to determine message metadata
    // 2. there were not enough bytes in previous binary
    if (_currentBinary.size() == 0) {
        _shouldParse = false;  // stop trying to parse
        return;                // no new data yet
    }
    // how many bytes we need off the wire
    auto partSize = _partialBinary.size();
    auto curSize = _currentBinary.size();
    auto totalNeed = _metadata.wireByteCount();

    if (totalNeed > partSize + curSize) {
        K2LOG_W(log::tx,
        "Received partial variable header continuation segment which doesn't have enough data: cursz={}, total={}, have={}",
        curSize, totalNeed, partSize);
        _setParserFailure(NonContinuationSegmentException());
        return;
    }
    K2ASSERT(log::tx, totalNeed <= txconstants::MAX_HEADER_SIZE, "invalid needed bytes determined: {}", totalNeed);
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
        data += sizeof(_metadata.payloadSize);
    }
    if (_metadata.isRequestIDSet()) {
        std::memcpy((char*)&_metadata.requestID, data, sizeof(_metadata.requestID));
        data += sizeof(_metadata.requestID);
    }
    if (_metadata.isResponseIDSet()) {
        std::memcpy((char*)&_metadata.responseID, data, sizeof(_metadata.responseID));
        data += sizeof(_metadata.responseID);
    }
    if (_metadata.isChecksumSet()) {
        std::memcpy((char*)&_metadata.checksum, data, sizeof(_metadata.checksum));
        data += sizeof(_metadata.checksum);
    }
    _pState = ParseState::WAIT_FOR_PAYLOAD;  // onto getting the payload
}

void RPCParser::_stWAIT_FOR_PAYLOAD() {
    // check to see if we're expecting payload
    if (!_metadata.isPayloadSizeSet()) {
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

    // check to see if we're already set
    if (needed == 0) {
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
}

void RPCParser::_stREADY_TO_DISPATCH() {
    if (_useChecksum && _payload && _metadata.isChecksumSet()) {
        auto checksum = _payload->computeCrc32c();
        if (checksum != _metadata.checksum) {
            _setParserFailure(ChecksumValidationException());
            return;
        }
    }
    _messageObserver(_fixedHeader.verb, std::move(_metadata), std::move(_payload));

    // only now we're ready to process the next message
    _pState = RPCParser::ParseState::WAIT_FOR_FIXED_HEADER;
}

void RPCParser::_stFAILED_STREAM() {
    K2LOG_W(log::tx, "Parsing of stream not possible");
    std::move(_partialBinary).prefix(0);
    std::move(_currentBinary).prefix(0);

    // release any partial data we have
    _shouldParse = false;  // stop trying to parse
    _parserFailureObserver(std::make_exception_ptr(_parserFailureException));
}

std::unique_ptr<Payload>
RPCParser::serializeMessage(Payload&& message, Verb verb, MessageMetadata metadata) {
    auto userMessageSize = message.getSize();
    auto&& buffers = message.release();
    auto metaPayloadSize = metadata.isPayloadSizeSet() ? metadata.payloadSize : 0;

    if (userMessageSize == metaPayloadSize) {
        // the incoming message doesn't have room for a header
        buffers.insert(buffers.begin(), Binary(txconstants::MAX_HEADER_SIZE));
        userMessageSize += txconstants::MAX_HEADER_SIZE;
    }

    K2ASSERT(log::tx, userMessageSize == metaPayloadSize + txconstants::MAX_HEADER_SIZE, "user message size mismatch");
    auto headerSize = RPCParser::serializeHeader(buffers[0], verb, std::move(metadata));
    return std::make_unique<Payload>(std::move(buffers), headerSize + metaPayloadSize);
}

std::vector<Binary>
RPCParser::prepareForSend(Verb verb, std::unique_ptr<Payload> payload, MessageMetadata metadata) {
    K2ASSERT(log::tx, payload->getSize() >= txconstants::MAX_HEADER_SIZE, "payload size too big");
    auto dataSize = payload->getSize() - txconstants::MAX_HEADER_SIZE;
    metadata.setPayloadSize(dataSize);
    if (_useChecksum) {
        // compute checksum starting at MAX_HEADER_SIZE until end of payload
        payload->seek(txconstants::MAX_HEADER_SIZE);
        auto checksum = payload->computeCrc32c();
        metadata.setChecksum(checksum);
    }
    // disassemble the payload so that we can write the header in the first binary
    auto&& buffers = payload->release();
    // write the header into the headroom of the first binary and remember to send the extra bytes
    dataSize += RPCParser::serializeHeader(buffers[0], verb, std::move(metadata));
    // write out the header and data
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
