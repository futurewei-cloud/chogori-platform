/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

#include "MPackSerialization.h"

namespace skv::http {

    bool MPackNodeReader::_readFromNode(DecimalD50& value) {
        K2LOG_V(log::mpack, "reading decimald50");
        DFReadStreamBuf readsb(*this);
        boost::archive::binary_iarchive bia(readsb, boost::archive::no_header);
        bia >> value;
        return true;
    }

    bool MPackNodeReader::_readFromNode(DecimalD100& value) {
        K2LOG_V(log::mpack, "reading decimald100");
        DFReadStreamBuf readsb(*this);
        boost::archive::binary_iarchive bia(readsb, boost::archive::no_header);
        bia >> value;
        return true;
    }


    void MPackNodeWriter::write(const DecimalD50& value) {
        K2LOG_V(log::mpack, "writing decimald50 type {}", value);
        DFWriteStreamBuf writesb(*this);
        boost::archive::binary_oarchive boa(writesb, boost::archive::no_header);
        boa << value;
    }
    void MPackNodeWriter::write(const DecimalD100& value) {
        K2LOG_V(log::mpack, "writing decimald100 type {}", value);
        DFWriteStreamBuf writesb(*this);
        boost::archive::binary_oarchive boa(writesb, boost::archive::no_header);
        boa << value;
    }

}