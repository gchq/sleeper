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
#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/logging_resource_adaptor.hpp>
#include <rmm/mr/device/per_device_resource.hpp>
#include <string>
#include <vector>

inline constexpr ::size_t DEFAULT_MAX_SAFE_LIMIT = 2;
inline constexpr ::size_t DEFAULT_FOOTER_SIZE = 1'048'576 * 2;  // grab last 2MiB of Parquet files

inline const std::string PARQUET_EXTENSION = "parquet";

template <typename T>
void printVec(std::vector<T> const& vec) noexcept {
    std::cerr << "{ ";
    for (auto const& v : vec) {
        std::cerr << v << ", ";
    }
    std::cerr << " }\n";
}
