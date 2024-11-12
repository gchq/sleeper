#pragma once

#include <cudf/scalar/scalar.hpp>
#include <cudf/table/table_view.hpp>

#include <utility>
#include <vector>

int convertInteger(cudf::scalar const &scalar);

std::pair<std::vector<cudf::table_view>, std::vector<cudf::table_view>> splitAtNeedle(cudf::table_view const &needle,
  std::vector<cudf::table_view> const &haystacks);
