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
    std::unique_ptr<cudf::io::datasource> src;
    std::string file;

  public:
    PrefetchingSource(std::string_view path, std::unique_ptr<cudf::io::datasource> source) noexcept;

    // virtual ~PrefetchingSource() noexcept override;

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
};

}// namespace gpu_compact::io