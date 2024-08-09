#pragma once

#include "common_types.hpp"
#include "parquet_types.h"

#include <cstddef>
#include <cudf/ast/expressions.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>

#include <deque>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

using scalar_ptr = std::shared_ptr<cudf::scalar>;
using scalar_pair = std::tuple<std::string, scalar_ptr, std::string, scalar_ptr>;

std::deque<scalar_pair> getRanges(std::vector<page_info> const &pages,
  cudf::size_type sort_col,
  parquet::format::Type::type col_type,
  parquet::format::ConvertedType::type conv_type,
  size_t chunk_read_limit,
  std::vector<std::vector<parquet::format::ColumnIndex>> const &indexes_per_file);
