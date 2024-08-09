
#pragma once

#include "parquet_types.h"

#include <cudf/ast/expressions.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>


std::unique_ptr<cudf::table> filter_table_by_range(cudf::table_view const &input,
  cudf::size_type sort_col_id,
  std::shared_ptr<cudf::scalar> const &low,
  std::shared_ptr<cudf::scalar> const &high);

// AST support

struct literal_converter
{
    template<typename T> static constexpr bool is_supported()
    {
        return std::is_same_v<T, cudf::string_view> || (cudf::is_fixed_width<T>() && !cudf::is_fixed_point<T>());
    }

    template<typename T, std::enable_if_t<is_supported<T>()> * = nullptr>
    cudf::ast::literal operator()(cudf::scalar &_value)
    {
        using scalar_type = cudf::scalar_type_t<T>;
        auto &low_literal_value = static_cast<scalar_type &>(_value);
        return cudf::ast::literal(low_literal_value);
    }

    template<typename T, std::enable_if_t<!is_supported<T>()> * = nullptr>
    cudf::ast::literal operator()(cudf::scalar &_value)
    {
        CUDF_FAIL("Unsupported type for literal");
    }
};

std::string
  to_string(std::string val, parquet::format::Type::type col_type, parquet::format::ConvertedType::type conv_type);

std::shared_ptr<cudf::scalar>
  to_scalar(std::string val, parquet::format::Type::type col_type, parquet::format::ConvertedType::type conv_type);

std::shared_ptr<cudf::scalar> min_for_type(parquet::format::Type::type col_type,
  parquet::format::ConvertedType::type conv_type);
std::shared_ptr<cudf::scalar> max_for_type(parquet::format::Type::type col_type,
  parquet::format::ConvertedType::type conv_type);