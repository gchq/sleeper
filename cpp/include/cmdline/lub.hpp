#pragma once

#include <cudf/table/table.hpp>

#include <cstddef>
#include <memory>
#include <vector>

std::size_t findLeastUpperBound(std::vector<std::unique_ptr<cudf::table>> const &tables, std::size_t const colNo = 0);
