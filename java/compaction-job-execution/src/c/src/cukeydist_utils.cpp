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
#include "cukeydist_utils.hpp"

#include <fcntl.h>
#include <parquet/metadata.h>
#include <parquet/types.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <functional>
#include <iostream>
#include <memory>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/per_device_resource.hpp>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "histo.hpp"
#include "s3_utils.hpp"
#include "s3_writer.hpp"
#include "string_helpers.hpp"

double timestamp() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<double>(tv.tv_sec) + 0.000001 * static_cast<double>(tv.tv_usec);
}

bool isS3Path(std::string const& path) noexcept {
    return startsWith(path, S3_URI_PREFIX);
}

::size_t getFreeMem(rmm::cuda_stream_view const stream,
                    rmm::mr::device_memory_resource* const mem) {
    return mem->get_mem_info(stream).first;
}

void freeMemory(std::string const& prefix,
                rmm::cuda_stream_view const stream,
                rmm::mr::device_memory_resource* const mem) {
    std::cerr << "Free memory: " << prefix << " " << getFreeMem(stream, mem) << std::endl;
}

void prefetch(std::string const& file, off_t const offset, ::size_t const length) {
    // std::cerr << "Background prefetch " << file << " from " << offset << " size " << length << std::endl;
    int fd = open(file.c_str(), O_RDONLY);
    mmap(nullptr, length, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, offset);
    close(fd);
}

void start_prefetchers(std::vector<std::string> const& filePaths) {
    std::vector<::uintmax_t> sizes{};
    for (auto const& file : filePaths) {
        ::uintmax_t fileSize = std::filesystem::file_size(file);
        sizes.emplace_back(fileSize);
    }
    if (sizes.empty()) {  // nothing to do!
        return;
    }
    ::uintmax_t maxSize = *std::max_element(sizes.cbegin(), sizes.cend());
    for (::uintmax_t part = 0; part < maxSize; part += BATCH_SIZE) {
        std::vector<std::thread> threadList;
        for (::size_t i = 0; i < filePaths.size(); ++i) {
            // check to see if each file has still got a data left to fetch
            if (part < sizes[i]) {
                threadList.emplace_back(prefetch, std::cref(filePaths[i]), part, BATCH_SIZE);
            }
        }
        // wait for all threads to terminate
        for (auto& t : threadList) {
            t.join();
        }
    }
}

static cudf::io::compression_type convertCompressionType(std::string const& compression) {
    if (iequals(compression, SNAPPY)) {
        return cudf::io::compression_type::SNAPPY;
    } else if (iequals(compression, ZSTD)) {
        return cudf::io::compression_type::ZSTD;
    } else if (iequals(compression, NONE)) {
        return cudf::io::compression_type::NONE;
    } else {
        return cudf::io::compression_type::AUTO;
    }
}

static ::size_t outputCounter = 0;
std::unique_ptr<cudf::io::parquet_chunked_writer> createWriter(
    CommandLineInput const& options,
    cudf::io::table_input_metadata const& tim,
    AwsLibrary* library,
    std::shared_ptr<S3Sink>& currentSink) {
    
    std::string newFile = options.outputFiles.at(outputCounter++);
    cudf::io::sink_info destination{};
    if (isS3Path(newFile)) {
        // create a new sink
        currentSink = std::make_shared<S3Sink>(library->getS3Client(), newFile);
        destination = cudf::io::sink_info(currentSink.get());
    } else {
        destination = cudf::io::sink_info(newFile);
    }
    auto wopts = cudf::io::chunked_parquet_writer_options::builder(destination);
    wopts.metadata(&tim)
        .compression(convertCompressionType(options.codec))
        .row_group_size_bytes(options.rowGroupBytes)
        .row_group_size_rows(options.rowGroupRows)
        .max_page_size_bytes(options.pageBytes);
    return std::make_unique<cudf::io::parquet_chunked_writer>(wopts.build());
}

::size_t findVariantIndex(std::shared_ptr<parquet::FileMetaData> const metadata, CommandLineInput const& opts) {
    auto colType = metadata->schema()->Column(opts.sortIndexes[0])->physical_type();
    if (colType == parquet::Type::type::BYTE_ARRAY) {
        return 0;
    } else if (colType == parquet::Type::type::INT32) {
        return 1;
    } else if (colType == parquet::Type::type::INT64) {
        return 2;
    } else {
        CUDF_FAIL("Specified sort column is not string, int32 or int64");
    }
}
