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

#include "PayloadFileUtil.h"

namespace k2 {

// create a directory if it doesn't exist already
bool fileutil::makeDir(String path, int mode) {
    if (::mkdir(path.c_str(), mode) != 0) {
        if (errno != EEXIST) {
            K2ERROR("Unable to create directory " << path << ": " << strerror(errno));
            return false;
        }
    }
    return true;
}

// check to see if a file exists
bool fileutil::fileExists(String path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno != ENOENT) {
            K2ERROR("problem reading file: name=" << path << ":: " << strerror(errno));
        }
        return false;
    }
    ::close(fd);
    return true;
}

// read an entire file into a payload
bool fileutil::readFile(Payload& payload, String path) {
    if (!fileExists(path)) return false;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        K2ERROR("error openning file: " << path << " -->" << strerror(errno));
        return false;
    }

    payload.clear();
    bool success = false;

    Defer d([&] {
        if (!success) {
            payload.clear();
        }
        ::close(fd);
    });

    while (1) {
        Binary buf(4096);
        auto rd = ::read(fd, buf.get_write(), buf.size());
        if (rd < 0) {
            K2ERROR("problem reading file: " << strerror(errno));
            success = false;
            break;
        }
        if (rd == 0) {
            success = true;
            break;
        }
        buf.trim(rd);
        payload.appendBinary(std::move(buf));
    }
    return success;
}

// write an entire payload into the given file
bool fileutil::writeFile(Payload&& payload, String path) {
    payload.truncateToCurrent();
    auto leftBytes = payload.getSize();

    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY);
    if (fd < 0) {
        K2ERROR("Unable to open file for writing: name=" << path << ", err=" << strerror(errno));
        return false;
    }

    Defer d([&] {
        ::close(fd);
    });

    for (auto&& buf : payload.release()) {
        auto towrite = std::min(buf.size(), leftBytes);
        size_t written = ::write(fd, buf.get(), towrite);
        if (written != towrite) {
            return false;
        }
        leftBytes -= written;
        if (leftBytes == 0) break;
    }
    return true;
}

} // ns k2
