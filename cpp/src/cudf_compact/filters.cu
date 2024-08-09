
#include "filters.h"

#include <cudf/ast/expressions.hpp>
#include <cudf/scalar.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/transform.hpp>
#include <cudf/utilities/type_dispatcher.hpp>

#include <exception>

std::unique_ptr<cudf::table> filter_table_by_range(cudf::table_view const &input,
  cudf::size_type sort_col_id,
  std::shared_ptr<cudf::scalar> const &low,
  std::shared_ptr<cudf::scalar> const &high)
{
    auto filter_col = cudf::ast::column_reference(sort_col_id);
    auto lo_lit = cudf::type_dispatcher(low->type(), literal_converter{}, *low);
    auto hi_lit = cudf::type_dispatcher(high->type(), literal_converter{}, *high);

    // colval >= low
    auto expr_1 = cudf::ast::operation(cudf::ast::ast_operator::GREATER_EQUAL, filter_col, lo_lit);
    // colval < high
    auto expr_2 = cudf::ast::operation(cudf::ast::ast_operator::LESS, filter_col, hi_lit);
    // expr_1 && expr_2
    auto expr_3 = cudf::ast::operation(cudf::ast::ast_operator::LOGICAL_AND, expr_1, expr_2);
    auto result = cudf::compute_column(input, expr_3);
    return cudf::apply_boolean_mask(input, result->view());
}

// AST support

std::string
  to_string(std::string val, parquet::format::Type::type col_type, parquet::format::ConvertedType::type conv_type)
{
    if (val == "-inf" || val == "inf") return val;
    switch (col_type) {
    case parquet::format::Type::INT32: {
        int32_t const *ptr = reinterpret_cast<int32_t const *>(val.data());
        int32_t const ival = *ptr;
        return std::to_string(ival);
    }
    case parquet::format::Type::INT64: {
        int64_t const *ptr = reinterpret_cast<int64_t const *>(val.data());
        int64_t const ival = *ptr;
        return std::to_string(ival);
    }
    case parquet::format::Type::BYTE_ARRAY:
        return val;
    default:
        SPDLOG_CRITICAL("unknown/unsupported type {}", col_type);
        throw std::runtime_error();
    }
}

std::shared_ptr<cudf::scalar>
  to_scalar(std::string val, parquet::format::Type::type col_type, parquet::format::ConvertedType::type conv_type)
{
    switch (col_type) {
    case parquet::format::Type::INT32: {
        int32_t const *ptr = reinterpret_cast<int32_t const *>(val.data());
        int32_t const ival = *ptr;
        return std::make_shared<cudf::numeric_scalar<int32_t>>(ival);
    }
    case parquet::format::Type::INT64: {
        int64_t const *ptr = reinterpret_cast<int64_t const *>(val.data());
        int64_t const ival = *ptr;
        return std::make_shared<cudf::numeric_scalar<int64_t>>(ival);
    }
    case parquet::format::Type::BYTE_ARRAY:
        return std::make_shared<cudf::string_scalar>(val);
    default:
        SPDLOG_CRITICAL("unknown/unsupported type {}", col_type);
        throw std::runtime_error();
    }
}

// TODO: support more types
std::shared_ptr<cudf::scalar> min_for_type(parquet::format::Type::type col_type,
  parquet::format::ConvertedType::type conv_type)
{
    using parquet::format::Type;
    switch (col_type) {
    case Type::INT32:
        return std::make_shared<cudf::numeric_scalar<int32_t>>(std::numeric_limits<int32_t>::min());
    case Type::INT64:
        return std::make_shared<cudf::numeric_scalar<int64_t>>(std::numeric_limits<int64_t>::min());
    case Type::BYTE_ARRAY:
        return std::make_shared<cudf::string_scalar>("");
    default:
        SPDLOG_CRITICAL("unknown/unsupported type {}", col_type);
        throw std::runtime_error();
    }
}

std::shared_ptr<cudf::scalar> max_for_type(parquet::format::Type::type col_type,
  parquet::format::ConvertedType::type conv_type)
{
    using parquet::format::Type;
    switch (col_type) {
    case Type::INT32:
        return std::make_shared<cudf::numeric_scalar<int32_t>>(std::numeric_limits<int32_t>::max());
    case Type::INT64:
        return std::make_shared<cudf::numeric_scalar<int64_t>>(std::numeric_limits<int64_t>::max());
    case Type::BYTE_ARRAY:
        return std::make_shared<cudf::string_scalar>("\xff\xff\xff\xff");
    default:
        SPDLOG_CRITICAL("unknown/unsupported type {}", col_type);
        throw std::runtime_error();
    }
}
