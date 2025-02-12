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
#pragma once

#include <cudf/io/datasource.hpp>
#include <rmm/cuda_stream_view.hpp>

#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <string_view>

using std::uint8_t;

namespace gpu_compact::io
{

struct PrefetchingSource final : public cudf::io::datasource
{
  private:
    bool enablePrefetch;
    std::unique_ptr<cudf::io::datasource> src;
    std::string file;
    void prefetch(::size_t const offset, ::size_t const len);

  public:
    PrefetchingSource(std::string_view path,
      std::unique_ptr<cudf::io::datasource> source,
      bool prefetch = true) noexcept;

    std::unique_ptr<cudf::io::datasource::buffer> host_read(::size_t offset, ::size_t size) override;

    ::size_t host_read(::size_t offset, ::size_t size, uint8_t *dst) override;

    [[nodiscard]] bool supports_device_read() const noexcept override;

    [[nodiscard]] bool is_device_read_preferred(::size_t size) const noexcept override;

    std::unique_ptr<cudf::io::datasource::buffer>
      device_read(::size_t offset, ::size_t size, rmm::cuda_stream_view stream) override;

    ::size_t device_read(::size_t offset, ::size_t size, uint8_t *dst, rmm::cuda_stream_view stream) override;

    std::future<::size_t>
      device_read_async(::size_t offset, ::size_t size, uint8_t *dst, rmm::cuda_stream_view stream) override;

    [[nodiscard]] ::size_t size() const noexcept override;

    [[nodiscard]] bool is_empty() const noexcept override;

    bool prefetch() noexcept;

    void prefetch(bool enable) noexcept;
};

}// namespace gpu_compact::io