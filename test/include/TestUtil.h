#pragma once
#include <k2/common/Common.h>
#include <k2/common/Log.h>
#include <pthread.h>
#include <filesystem>
#include <iostream>

std::string generateTempFolderPath(const char* id)
{
    char folder[255];
    int res = snprintf(folder, sizeof(folder), "/%s_%lx%x%x/", id ? id : "", pthread_self(), (uint32_t)time(nullptr), (uint32_t)rand());
    K2ASSERT(res > 0 && res < (int)sizeof(folder), "unable to construct folder name");

    return std::filesystem::temp_directory_path().concat(folder);
}
