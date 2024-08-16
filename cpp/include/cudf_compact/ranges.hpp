#pragma once

#include <cudf/ast/expressions.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>

#include "cudf_compact/common_types.hpp"
#include "cudf_compact/parquet_types.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

namespace gpu_compact::cudf_compact {

std::deque<scalar_pair> getRanges(std::vector<page_info> const &pages,
  cudf::size_type sort_col,
  parquet::format::Type::type col_type,
  parquet::format::ConvertedType::type conv_type,
  size_t chunk_read_limit,
  std::vector<std::vector<parquet::format::ColumnIndex>> const &indexes_per_file);

} // namespace gpu_compact::cudf_compact
