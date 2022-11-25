#pragma once
#include <cudf/column/column_view.hpp>
#include <vector>

template <typename T>
std::vector<T> getFirstLast(cudf::column_view const& firstColumn);