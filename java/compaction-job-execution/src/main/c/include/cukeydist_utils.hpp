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
#include <parquet/metadata.h>

#include <cstddef>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <memory>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/per_device_resource.hpp>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <stdexcept>

#include "cukeydist_types.hpp"
#include "s3_utils.hpp"
#include "s3_writer.hpp"

inline const std::string S3_URI_PREFIX = "s3://";
inline constexpr ::uintmax_t BATCH_SIZE = 1'048'576 * 100;

inline const std::string ZSTD = "ZSTD";
inline const std::string SNAPPY = "SNAPPY";
inline const std::string NONE = "NONE";

double timestamp();

bool isS3Path(std::string const& path) noexcept;

void freeMemory(std::string const& prefix = "unknown",
                rmm::cuda_stream_view const stream = rmm::cuda_stream_default,
                rmm::mr::device_memory_resource* const mem = rmm::mr::get_current_device_resource());

::size_t getFreeMem(rmm::cuda_stream_view const stream = rmm::cuda_stream_default,
                    rmm::mr::device_memory_resource* const mem = rmm::mr::get_current_device_resource());

void prefetch(std::string const& file, off_t const offset, ::size_t const length);

void start_prefetchers(std::vector<std::string> const& filePaths);

std::unique_ptr<cudf::io::parquet_chunked_writer> createWriter(
    CommandLineInput const & options,
    cudf::io::table_input_metadata const& tim,
    AwsLibrary* library,
    std::shared_ptr<S3Sink>& currentSink);
    
::size_t findVariantIndex(std::shared_ptr<parquet::FileMetaData> const metadata, CommandLineInput const& opts);

/**
 * @brief Creates and populates a variant of a default constructed type based upon the index.
 * 
 * @tparam T the std::variant type to populate
 * @tparam I index defaulted to 0, not intended to be set
 * @param index the type index for the variant
 * @return T populated variant
 */
template <typename T, ::size_t I = 0>
T populateVariant(::size_t const index) {
    if constexpr (I < std::variant_size_v<T>) {
        if (index == I) {
            return std::variant_alternative_t<I, T> {};
        }
        return populateVariant<T,I+1>(index);
    } else {
        throw std::invalid_argument{"index out of range for variant type"};
    }
}
