#pragma once
#include <pthread.h>
#include <iostream>
#include <filesystem>
#include <common/Common.h>

std::string generateTempFolderPath(const char* id)
{
    char folder[255];
    int res = snprintf(folder, sizeof(folder), "/%s_%lx%x%x/", id ? id : "", pthread_self(), (uint32_t)time(nullptr), (uint32_t)rand());
    ASSERT(res > 0 && res < (int)sizeof(folder));

    return std::filesystem::temp_directory_path().concat(folder);
}
