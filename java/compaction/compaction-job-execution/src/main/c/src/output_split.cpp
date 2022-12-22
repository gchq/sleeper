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
#include "output_split.hpp"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

std::vector<std::string> loadOutputPartsFile(fs::path const& path) {
    // test if file exists
    if (fs::exists(path)) {
        // load the file
        std::ifstream partFile{path};
        std::vector<std::string> parts{};
        std::string line;
        while (std::getline(partFile, line)) {
            // don't add blank lines or ones that start with #
            if (!(line.empty() || line.front() == '#')) {
                parts.push_back(std::move(line));
            }
        }
        return parts;
    } else {
        // if not, then return empty optional
        return {};
    }
}
