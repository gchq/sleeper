#pragma once
#include <cudf/table/table_view.hpp>

#include <cstddef>
#include <vector>

::size_t findLeastUpperBound(std::vector<cudf::table_view> const &views, ::size_t const colNo = 0);
