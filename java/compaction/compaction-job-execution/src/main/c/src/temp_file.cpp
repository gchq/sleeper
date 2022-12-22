/*
 * Copyright 2022 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "temp_file.hpp"

#include <cstddef>
#include <cstdlib>  //getenv, at_exit
#include <filesystem>
#include <mutex>
#include <random>
#include <string>
#include <vector>
#include <iostream>

namespace fs=std::filesystem;

static fs::path tempPath() {
    static char * userTempPath = ::getenv(USER_TEMP);
    if (userTempPath) {
        return userTempPath;
    } else {
        return fs::temp_directory_path();
    }
}

static std::string randomString(::size_t len) {
    static std::mt19937 generator{std::random_device{}()};
    static std::uniform_int_distribution<int> distribution{'a', 'z'};

    std::string baseStr(len, '\0');
    for(auto& s: baseStr) {
        s = distribution(generator);
    }
    return baseStr;
}

static std::vector<fs::path> tempFiles{};

static void deleteTempFiles() noexcept {
    for (auto const & p : tempFiles) {
        try {
            if (fs::exists(p)) {
                fs::remove(p); //throw away return value
            }
        } catch (fs::filesystem_error & e) {
            std::cerr << "Couldn't delete " << p << " because " << e.what() << std::endl;
        }
    }
}

static bool registerDeletion() noexcept {
    ::atexit(deleteTempFiles);
    return true;
}

fs::path generateLocalFileName() {
    //global list
    static std::mutex tempListMutex{};
    static bool const regFlag=registerDeletion(); //guarantee only one thread calls this
    fs::path tempFile=tempPath();
    tempFile /= randomString(TEMP_LEN);
    {
        //modify vector under lock
        std::lock_guard lock{tempListMutex};
        // now add that to the list of things to be deleted later
        tempFiles.push_back(tempFile);
    }
    return tempFile;
}
