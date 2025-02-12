/*
 * Copyright 2022-2025 Crown Copyright
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
#include "io/s3_utils.hpp"

#include <fmt/core.h>

#include <exception>

namespace gpu_compact::s3
{

Aws::SDKOptions const &getOptions() noexcept {
    static Aws::SDKOptions options = []() noexcept {
        Aws::SDKOptions v;
        v.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
        return v;
    }();
    return options;
}

void initialiseAWS() {
    Aws::InitAPI(getOptions());
}

void shutdownAWS() noexcept {
    Aws::ShutdownAPI(getOptions());
}

std::shared_ptr<Aws::S3::S3Client> makeClient() {
    return Aws::MakeShared<Aws::S3::S3Client>("");
}

Aws::String get_part(std::string_view s, std::smatch::size_type const group) {
    if (std::match_results<std::string_view::const_iterator> mr; std::regex_match(s.cbegin(), s.cend(), mr, S3_CHECK)) {
        if (mr.size() < 4) {
            throw std::logic_error("S3 URL regex failed to find 4 match groups!");
        } else {
            return mr.str(group);
        }
    } else {
        throw std::runtime_error(fmt::format("{} is not a valid S3 URL", s));
    }
}

}// namespace gpu_compact::s3