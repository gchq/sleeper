#include "io/prefetch.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <thread>

namespace gpu_compact::io
{

static constinit const ::size_t READAHEAD_MIN = 2000 * 1'048'576ul;
static constinit const ::size_t BUF_SIZE = 512 * 1'024;

void prefetch(std::string const &file, ::size_t const offset, ::size_t const len) {
    SPDLOG_INFO("Prefetch of {} offset {:d} len {:d}", std::filesystem::path(file).filename().string(), offset, len);
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

void prefetch_async(std::string const &file, ::size_t const offset, ::size_t const len) {
    std::jthread(prefetch, file, offset, len).detach();
}

}// namespace gpu_compact::io
