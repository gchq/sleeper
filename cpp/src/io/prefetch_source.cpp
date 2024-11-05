#include "io/prefetch_source.hpp"

#include "io/prefetch.hpp"

#include <utility>

namespace gpu_compact::io
{

void PrefetchingSource::prefetch(::size_t const offset, ::size_t const len) {
    std::cout << "Read request " << file << " offset " << offset << " to " << (offset + len) << " len " << len << "\n";
    if (enablePrefetch) {
        prefetch_read_async(file, offset /*+ len*/, len * 2 /*+ 100 * 1'024ul * 1'024ul*/);
    }
}

PrefetchingSource::PrefetchingSource(std::string_view path, std::unique_ptr<cudf::io::datasource> source) noexcept
  : enablePrefetch(true), src(std::move(source)), file(path) {}

std::unique_ptr<cudf::io::datasource::buffer> PrefetchingSource::host_read(::size_t offset, ::size_t size) {
    prefetch(offset, size);
    return src->host_read(offset, size);
}

::size_t PrefetchingSource::host_read(::size_t offset, ::size_t size, uint8_t *dst) {
    prefetch(offset, size);
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
    prefetch(offset, size);
    return src->device_read(offset, size, stream);
}

::size_t PrefetchingSource::device_read(::size_t offset, ::size_t size, uint8_t *dst, rmm::cuda_stream_view stream) {
    prefetch(offset, size);
    return src->device_read(offset, size, dst, stream);
}

std::future<::size_t>
  PrefetchingSource::device_read_async(::size_t offset, ::size_t size, uint8_t *dst, rmm::cuda_stream_view stream) {
    prefetch(offset, size);
    return src->device_read_async(offset, size, dst, stream);
}

[[nodiscard]] ::size_t PrefetchingSource::size() const noexcept {
    return src->size();
}

[[nodiscard]] bool PrefetchingSource::is_empty() const noexcept {
    return src->is_empty();
}

bool PrefetchingSource::prefetch() noexcept {
    return enablePrefetch;
}

void PrefetchingSource::prefetch(bool enable) noexcept {
    enablePrefetch = enable;
}

}// namespace gpu_compact::io
