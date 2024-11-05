#include "io/prefetch.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>

namespace gpu_compact::io
{

static constinit const ::size_t BUF_SIZE = 512 * 1'024;

void prefetch_mmap(std::string const &file, ::size_t const offset, ::size_t const len) {
    // Ask OS for page size
    static auto const pageSize = static_cast<::size_t>(sysconf(_SC_PAGE_SIZE));
    auto readaheadOffset = offset;
    // Align offset to page boundary
    readaheadOffset = (readaheadOffset / pageSize) * pageSize;

    SPDLOG_INFO("Prefetch of {} offset {:d} to {:d} len {:d}",
      std::filesystem::path(file).filename().string(),
      readaheadOffset,
      readaheadOffset + len,
      len);
    // Open file
    int fd = ::open(file.data(), O_RDONLY);
    if (fd < 0) {
        SPDLOG_ERROR("Couldn't open {} ", file);
        return;
    }
    // set-up a finaliser to close file
    std::unique_ptr<int, std::function<void(int *)>> closer{ &fd, [&file](int *f) {
                                                                if (f != nullptr && ::close(*f)) {
                                                                    SPDLOG_WARN("Couldn't close {}", file);
                                                                }
                                                            } };
    // Memory map file
    void *ptr = ::mmap(nullptr, len, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, static_cast<off_t>(readaheadOffset));
    if (ptr == MAP_FAILED) {
        SPDLOG_ERROR("Couldn't memory map {}: {}", file, ::strerror(errno));
        return;
    }
    // Unmap file
    if (::munmap(ptr, len) != 0) {
        SPDLOG_WARN("Couldn't unmap {}", file);
        // Couldn't unmap file: ignore
    }
}

void prefetch_read(std::string const &file, ::size_t const offset, ::size_t const len) {
    SPDLOG_INFO("Prefetch of {} offset {:d} to {:d} len {:d}",
      std::filesystem::path(file).filename().string(),
      offset,
      offset + len,
      len);
    // Open file
    std::ifstream in{ file, std::ios::in | std::ios::binary };
    // Seek to position
    in.seekg(static_cast<std::ifstream::off_type>(offset), std::ios::beg);

    // Repeat buffer read
    ::size_t count = 0;
    auto const buffer = std::make_unique<char[]>(BUF_SIZE);
    while (in && count < len) {
        in.read(buffer.get(), BUF_SIZE);
        auto readCount = in.gcount();
        if (readCount > 0) {
            count += static_cast<::size_t>(readCount);
        } else {
            SPDLOG_WARN("Got negative gcount()");
        }
    }
}

void prefetch_read_async(std::string const &file, ::size_t const offset, ::size_t const len) {
    std::jthread(prefetch_read, file, offset, len).detach();
}

void prefetch_mmap_async(std::string const &file, ::size_t const offset, ::size_t const len) {
    std::jthread(prefetch_mmap, file, offset, len).detach();
}

}// namespace gpu_compact::io
