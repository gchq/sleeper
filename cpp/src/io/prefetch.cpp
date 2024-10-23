#include "io/prefetch.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>

namespace gpu_compact::io
{

static constinit const ::size_t READAHEAD_LEN = 3000 * 1'048'576ul;

void prefetch(std::string const &file, ::size_t const offset, ::size_t const len) {
    // Ask OS for page size
    static auto const pageSize = static_cast<::size_t>(sysconf(_SC_PAGE_SIZE));
    auto readaheadOffset = offset + len;
    // Align offset to page boundary
    readaheadOffset = (readaheadOffset / pageSize) * pageSize;
    auto const readaheadLen = std::max(len * 2, READAHEAD_LEN);
    SPDLOG_INFO("Prefetch of {} offset {:d} len {:d} from request of offset {:d} len {:d}",
      std::filesystem::path(file).filename().string(),
      readaheadOffset,
      readaheadLen,
      offset,
      len);
    // Open file
    int fd = ::open(file.data(), O_RDONLY);
    if (fd < 0) {
        SPDLOG_ERROR("Couldn't open {} ", file);
        return;
    }
    // set-up a finaliser to close file
    std::unique_ptr<int, std::function<void(int *)>> closer{ &fd, [&file](int *f) {
                                                                if (*f && ::close(*f)) {
                                                                    SPDLOG_WARN("Couldn't close {}", file);
                                                                }
                                                            } };
    // Memory map file
    void *ptr =
      ::mmap(nullptr, readaheadLen, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, static_cast<off_t>(readaheadOffset));
    if (ptr == MAP_FAILED) {
        SPDLOG_ERROR("Couldn't memory map {}: {}", file, ::strerror(errno));
        return;
    }
    // Unmap file
    if (::munmap(ptr, readaheadLen) != 0) {
        SPDLOG_WARN("Couldn't unmap {}", file);
        // Couldn't unmap file: ignore
    }
}

void prefetch_async(std::string const &file, ::size_t const offset, ::size_t const len) {
    std::jthread(prefetch, file, offset, len).detach();
}
}// namespace gpu_compact::io
