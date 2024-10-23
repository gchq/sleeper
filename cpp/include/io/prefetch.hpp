#pragma once

#include <cstddef>
#include <string>

namespace gpu_compact::io
{

void prefetch(std::string const &file, ::size_t const offset, ::size_t const len);

void prefetch_async(std::string const &file, ::size_t const offset, ::size_t const len);

}// namespace gpu_compact::io