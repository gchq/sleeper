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
#include <cudf/io/datasource.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <future>
#include <memory>
#include <string>

struct S3WaitingSource : public cudf::io::datasource {
   private:
    std::unique_ptr<cudf::io::datasource> source;
    std::shared_ptr<std::atomic_size_t> downloadedAmount;

    void waitUntil(::size_t const offset, ::size_t const size) const noexcept;
    inline static std::string const MAGIC = "PAR1";

   public:
    S3WaitingSource(std::unique_ptr<cudf::io::datasource>&& _source,
                    std::shared_ptr<std::atomic_size_t> _downloadedAmount) noexcept;
    // make this moveable
    S3WaitingSource(S3WaitingSource&&) = default;

    ::size_t host_read(::size_t offset, ::size_t size, ::uint8_t* dst) override;

    std::unique_ptr<cudf::io::datasource::buffer> host_read(::size_t offset, ::size_t size) override;

    [[nodiscard]] bool supports_device_read() const override;

    ::size_t device_read(::size_t offset,
                         ::size_t size,
                         ::uint8_t* dst,
                         rmm::cuda_stream_view stream) override;

    std::unique_ptr<buffer> device_read(::size_t offset, ::size_t size, rmm::cuda_stream_view stream) override;

    [[nodiscard]] ::size_t size() const override;

    bool is_device_read_preferred(::size_t size) const override;

    std::future<::size_t> device_read_async(::size_t offset, ::size_t size, ::uint8_t* dst, rmm::cuda_stream_view stream) override;

    bool is_empty() const override;
};
