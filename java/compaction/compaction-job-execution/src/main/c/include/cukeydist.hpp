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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cudf/types.hpp> //cudf size_type
#include <iostream>
#include <parquet/metadata.h>
#include <parquet/types.h>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/logging_resource_adaptor.hpp>
#include <rmm/mr/device/per_device_resource.hpp>
#include <string>
#include <vector>

#include "cukeydist_types.hpp"
#include "s3_utils.hpp"
#include "s3_writer.hpp"

inline constexpr ::size_t DEFAULT_MAX_SAFE_LIMIT = 2;
inline constexpr ::size_t DEFAULT_FOOTER_SIZE = 1'048'576 * 2; // grab last 2MiB of Parquet files

inline const std::string PARQUET_EXTENSION = "parquet";

template <typename T> void printVec(std::vector<T> const &vec) noexcept {
    std::cerr << "{ ";
    for (auto const &v : vec) {
        std::cerr << v << ", ";
    }
    std::cerr << " }\n";
}

void readRowGroupStats(std::vector<cudf::size_type> const &sortCols,
                       std::shared_ptr<parquet::FileMetaData> const fmeta,
                       SupportedTypes::EdgePair_vec2_variant &fileStatsVariant,
                       std::vector<fileinfo_t> &sources,
                       SupportedTypes::Edge_vec_variant &rowGroupParts, std::string const &file,
                       ::size_t const fileIndex);

CommandLineOutput compactFiles(SupportedTypes::Edge_vec_variant const &rowGroupParts,
                               CommandLineInput const &opts, std::vector<fileinfo_t> const &sources,
                               SupportedTypes::EdgePair_vec2_variant const &fileStats,
                               std::shared_ptr<AwsLibrary> awsPtr,
                               cudf::io::table_input_metadata const &tim,
                               std::shared_ptr<S3Sink> &currentSink,
                               std::shared_ptr<std::atomic_size_t> downloadedAmount);