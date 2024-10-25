#pragma once

#include <cstddef>
#include <string>

namespace gpu_compact::io
{

void prefetch_mmap(std::string const &file, ::size_t const offset, ::size_t const len);

void prefetch_mmap_async(std::string const &file, ::size_t const offset, ::size_t const len);

void prefetch_read(std::string const &file, ::size_t const offset, ::size_t const len);

void prefetch_read_async(std::string const &file, ::size_t const offset, ::size_t const len);

}// namespace gpu_compact::io