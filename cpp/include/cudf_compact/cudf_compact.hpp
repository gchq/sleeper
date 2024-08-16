#pragma once

#include <cudf/ast/expressions.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/strings/string_view.hpp>
#include <cudf/utilities/traits.hpp>
#include <rmm/cuda_device.hpp>
#include <rmm/mr/device/cuda_memory_resource.hpp>
#include <rmm/mr/device/owning_wrapper.hpp>
#include <rmm/mr/device/per_device_resource.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>
#include <spdlog/spdlog.h>

#include "cudf_compact/common_types.hpp"
#include "cudf_compact/cudf_utils.hpp"
#include "cudf_compact/parquet_types.h"

#include <algorithm>// std::ranges::equal
#include <chrono>
#include <locale>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

namespace gpu_compact::cudf_compact {

CompactionResult merge_sorted_files(CompactionInput const &inputData);

template<typename CharT>
[[nodiscard]] bool iequals(std::basic_string_view<CharT> lhs, std::basic_string_view<CharT> rhs) noexcept
{
    static auto defLocale = std::locale();
    return std::ranges::equal(
      lhs, rhs, [&](auto a, auto b) { return std::tolower(a, defLocale) == std::tolower(b, defLocale); });
}

// move most of below code to a CPP file
inline cudf::io::compression_type toCudfCompression(std::string_view compression) noexcept
{
    using namespace std::literals;
    if (iequals(compression, "uncompressed"sv)) {
        return cudf::io::compression_type::NONE;
    } else if (iequals(compression, "snappy"sv)) {
        return cudf::io::compression_type::SNAPPY;
    } else if (iequals(compression, "gzip"sv)) {
        return cudf::io::compression_type::GZIP;
    } else if (iequals(compression, "lzo"sv)) {
        return cudf::io::compression_type::LZO;
    } else if (iequals(compression, "lz4"sv)) {
        return cudf::io::compression_type::LZ4;
    } else if (iequals(compression, "brotli"sv)) {
        return cudf::io::compression_type::BROTLI;
    } else if (iequals(compression, "zstd"sv)) {
        return cudf::io::compression_type::ZSTD;
    } else if (iequals(compression, "snappy"sv)) {
        return cudf::io::compression_type::NONE;
    } else {
        SPDLOG_WARN("unrecognised compression type {}, using ZSTD");
        return cudf::io::compression_type::ZSTD;
    }
}

inline cudf::io::chunked_parquet_writer_options_builder write_opts(CompactionInput const &details,
  cudf::io::sink_info const &sink,
  cudf::io::table_input_metadata &&tim) noexcept
{
    using namespace std::literals;// for string_view
    // TODO: sanity check the input details here! static_casts from max row group sizes, etc.
    return cudf::io::chunked_parquet_writer_options::builder(sink)
      .metadata(std::move(tim))
      .compression(toCudfCompression(details.compression))
      .row_group_size_rows(static_cast<cudf::size_type>(details.maxRowGroupSize))
      .max_page_size_bytes(details.maxPageSize)
      .column_index_truncate_length(static_cast<int32_t>(details.columnTruncateLength))
      .stats_level(cudf::io::statistics_freq::STATISTICS_COLUMN)
      .write_v2_headers(iequals(static_cast<std::string_view>(details.writerVersion), "v2"sv))
      .dictionary_policy((details.dictEncRowKeys || details.dictEncSortKeys || details.dictEncValues)
                           ? cudf::io::dictionary_policy::ADAPTIVE
                           : cudf::io::dictionary_policy::NEVER);
}

inline std::unique_ptr<cudf::io::parquet_chunked_writer> make_writer(CompactionInput const &details,
  cudf::io::table_input_metadata &&tim)
{
    cudf::io::sink_info destination = make_sink_info(details.outputFile);
    auto wopts = write_opts(details, destination, std::move(tim));
    return std::make_unique<cudf::io::parquet_chunked_writer>(wopts.build());
}

struct literal_converter
{
    template<typename T> static constexpr bool is_supported() noexcept
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
    cudf::ast::literal operator()([[maybe_unused]] cudf::scalar &_value)
    {
        CUDF_FAIL("Unsupported type for literal");
    }
};

// make pooled memory resource and return a shared pointer to it. the shared resource must stay
// in scope for as long as it is needed by the underlying cudf library.
[[nodiscard]] inline auto make_pooled_mr()
{
    auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();
    auto mr =
      rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(cuda_mr, rmm::percent_of_free_device_memory(80));
    rmm::mr::set_current_device_resource(mr.get());
    return mr;
}

[[nodiscard]] inline std::chrono::time_point<std::chrono::steady_clock> timestamp() noexcept
{
    return std::chrono::steady_clock::now();
}

}// namespace gpu_compact::cudf_compact