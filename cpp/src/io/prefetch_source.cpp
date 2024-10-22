#include "io/prefetch_source.hpp"
#include <iostream>
#include <utility>

namespace gpu_compact::io
{

PrefetchingSource::PrefetchingSource(std::unique_ptr<cudf::io::datasource> &&source) noexcept
  : src(std::forward<decltype(source)>(source)) {}

// virtual ~PrefetchingSource::PrefetchingSource() noexcept override;

std::unique_ptr<cudf::io::datasource::buffer> PrefetchingSource::host_read(::size_t offset, ::size_t size) {
    return src->host_read(offset, size);
}

::size_t PrefetchingSource::host_read(::size_t offset, ::size_t size, uint8_t *dst) {
    return src->host_read(offset, size, dst);
}

[[nodiscard]] bool PrefetchingSource::supports_device_read() const noexcept {
    return src->supports_device_read();
}

[[nodiscard]] bool PrefetchingSource::is_device_read_preferred(::size_t size) const noexcept {
    return src->is_device_read_preferred(size);
}

std::unique_ptr<cudf::io::datasource::buffer>
  PrefetchingSource::device_read(::size_t offset, ::size_t size, rmm::cuda_stream_view stream) {
    return src->device_read(offset, size, stream);
}

::size_t PrefetchingSource::device_read(::size_t offset, ::size_t size, uint8_t *dst, rmm::cuda_stream_view stream) {
    return src->device_read(offset, size, dst, stream);
}

std::future<::size_t>
  PrefetchingSource::device_read_async(::size_t offset, ::size_t size, uint8_t *dst, rmm::cuda_stream_view stream) {
    return src->device_read_async(offset, size, dst, stream);
}

[[nodiscard]] ::size_t PrefetchingSource::size() const noexcept {
    return src->size();
}

[[nodiscard]] bool PrefetchingSource::is_empty() const noexcept {
    return src->is_empty();
}

}// namespace gpu_compact::io
