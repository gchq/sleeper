#pragma once
#include <cudf/table/table_view.hpp>

#include <vector>

void findLeastUpperBound(std::vector<cudf::table_view> const &views) noexcept;