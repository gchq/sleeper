#pragma once
#include <cudf/io/types.hpp>

#include <cstddef>
#include <vector>

::size_t findLeastUpperBound(std::vector<cudf::io::table_with_metadata> const &tables, ::size_t const colNo = 0);
