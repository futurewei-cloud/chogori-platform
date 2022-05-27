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
#include <k2/common/Common.h>
#include <k2/logging/Log.h>
#include <pthread.h>
#include <filesystem>
#include <iostream>
namespace k2::log {
inline thread_local k2::logging::Logger testutil("k2::testutil");
}
std::string generateTempFolderPath(const char* id)
{
    char folder[255];
    int res = snprintf(folder, sizeof(folder), "/%s_%lx%x%x/", id ? id : "", pthread_self(), (uint32_t)time(nullptr), (uint32_t)rand());
    K2ASSERT(k2::log::testutil, res > 0 && res < (int)sizeof(folder), "unable to construct folder name");

    return std::filesystem::temp_directory_path().concat(folder);
}
