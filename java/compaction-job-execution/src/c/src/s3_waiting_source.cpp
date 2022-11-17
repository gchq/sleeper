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
#include "s3_waiting_source.hpp"

#include <chrono>
#include <cstddef>
#include <cudf/io/datasource.hpp>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <utility>

S3WaitingSource::S3WaitingSource(std::unique_ptr<cudf::io::datasource>&& _source,
                                 std::shared_ptr<std::atomic_size_t> _downloadedAmount) noexcept
    : source(std::forward<decltype(_source)>(_source)),
      downloadedAmount(_downloadedAmount) {
}

::size_t S3WaitingSource::host_read(::size_t offset, ::size_t size, ::uint8_t* dst) {
    // std::cerr << "host_read( " << offset << " , " << size << " , " << dst << " )" << std::endl;
    // SPECIAL CASE: If this a read of the first four bytes of the file, then return a special
    //  buffer for the header magic word "PAR1"
    if (offset == 0 && size == 4) {
        std::uninitialized_copy(S3WaitingSource::MAGIC.cbegin(), S3WaitingSource::MAGIC.cend(), dst);
        std::cerr << "Returning magic header\n";
        return 4;
    }

    waitUntil(offset, size);
    return source->host_read(offset, size, dst);
}

std::unique_ptr<cudf::io::datasource::buffer> S3WaitingSource::host_read(::size_t offset, ::size_t size) {
    // std::cerr << "host_read( " << offset << " , " << size << " )" << std::endl;
    waitUntil(offset, size);
    return source->host_read(offset, size);
}

void S3WaitingSource::waitUntil(::size_t const offset, ::size_t const size) const noexcept {
    // can we cheat? We know the last 2MiB is downloaded first for the footer, so if the offset
    // is in that region, we can skip waiting
    if (offset >= (this->size() - (1'048'576 * 2))) {
        // std::cerr << "Don't need to wait for footer...\n";
        return;
    }
    ::size_t endByteExclusive = offset + size;  // might conceivably overflow
    if (endByteExclusive == 0) {
        return;  // can't wait for zero bytes to download
    }
    bool waited = false;
    while ((*downloadedAmount) < endByteExclusive) {
        if (!waited) {
            std::cerr << "Waiting for " << endByteExclusive << " bytes (excl.) to download currently at "
                      << *downloadedAmount << ".\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        waited = true;
    }
    if (waited) {
        // std::cerr << "done waiting.\n";
    }
}

[[nodiscard]] bool S3WaitingSource::supports_device_read() const {
    return source->supports_device_read();
}

::size_t S3WaitingSource::device_read(::size_t offset,
                                      ::size_t size,
                                      ::uint8_t* dst,
                                      rmm::cuda_stream_view stream) {
    waitUntil(offset, size);
    return source->device_read(offset, size, dst, stream);
}

std::unique_ptr<cudf::io::datasource::buffer> S3WaitingSource::device_read(::size_t offset, ::size_t size, rmm::cuda_stream_view stream) {
    waitUntil(offset, size);
    return source->device_read(offset, size, stream);
}

[[nodiscard]] ::size_t S3WaitingSource::size() const {
    return source->size();
}

bool S3WaitingSource::is_device_read_preferred(::size_t size) const {
    return source->is_device_read_preferred(size);
}

std::future<::size_t> S3WaitingSource::device_read_async(::size_t offset, ::size_t size, ::uint8_t* dst, rmm::cuda_stream_view stream) {
    waitUntil(offset, size);  // this really really isn't asynchronous anymore....
    return source->device_read_async(offset, size, dst, stream);
}

bool S3WaitingSource::is_empty() const {
    return source->is_empty();
}
