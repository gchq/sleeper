#include "cudf_compact/cudf_compact.hpp"

#include <cudf/ast/expressions.hpp>
#include <cudf/column/column.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/sorting.hpp>
#include <cudf/stream_compaction.hpp>
#include <cudf/strings/contains.hpp>
#include <cudf/strings/regex/regex_program.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/logger.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/device_memory_resource.hpp>
#include <spdlog/spdlog.h>

#include "cudf_compact/common_types.hpp"
#include "cudf_compact/filters.hpp"
#include "cudf_compact/metadata.hpp"
#include "cudf_compact/parquet_types.h"
#include "cudf_compact/ranges.hpp"
#include "format_helper/format_helper.hpp"
#include "io/prefetch_source.hpp"
#include "io/s3_utils.hpp"

#include <algorithm>// std::reduce
#include <cstddef>
#include <exception>
#include <memory>
#include <new>// std::bad_alloc
#include <optional>
#include <string>
#include <utility>
#include <vector>

#pragma GCC diagnostic ignored "-Wsign-conversion"
namespace gpu_compact::cudf_compact
{

cudf::io::table_with_metadata
  table_for_range(CompactionInput const &details, scalar_ptr const &low, scalar_ptr const &high) {
    // TODO replace with logic to find number for row key+sort key columns
    cudf::size_type const range_col = 0;

    // this filter will only prune at the row-group level. we'll later filter the resultant table
    // to remove individual rows.
    auto filter_col = cudf::ast::column_reference(range_col);
    auto lo_lit = cudf::type_dispatcher(low->type(), literal_converter{}, *low);
    auto hi_lit = cudf::type_dispatcher(high->type(), literal_converter{}, *high);
    auto expr_1 = cudf::ast::operation(cudf::ast::ast_operator::GREATER_EQUAL, filter_col, lo_lit);
    auto expr_2 = cudf::ast::operation(cudf::ast::ast_operator::LESS, filter_col, hi_lit);
    auto expr_3 = cudf::ast::operation(cudf::ast::ast_operator::LOGICAL_AND, expr_1, expr_2);

    std::vector<std::unique_ptr<cudf::io::datasource>> datasources{};
    std::vector<cudf::io::datasource *> source_pointers{};
    for (auto const &f : details.inputFiles) {
        // std::unique_ptr<cudf::io::datasource> source = cudf::io::datasource::create(f);
        auto source = std::make_unique<gpu_compact::io::PrefetchingSource>(f, cudf::io::datasource::create(f));
        source->prefetch(true);
        source_pointers.push_back(source.get());
        datasources.push_back(std::move(source));
    }

    auto si = cudf::io::source_info(source_pointers);
    // auto si = cudf::io::source_info(details.inputFiles);

    try {
        std::cout << ff("read table...\n");
        auto builder = cudf::io::parquet_reader_options::builder(si).filter(expr_3);
        auto table_with_metadata = cudf::io::read_parquet(builder.build());
        SPDLOG_INFO(ff("read {:Ld} rows", table_with_metadata.tbl->num_rows()));
        auto result = std::move(table_with_metadata.tbl);

        result = filter_table_by_range(result->view(), range_col, low, high);
        std::cout << ff("filter by range\n");
        if (result->num_rows() == 0)
            return { nullptr, {} };

        // do a simple regex filter
        // static std::unique_ptr<cudf::strings::regex_program> reg_filter = cudf::strings::regex_program::create(
        //   "^.+(?:aa).*(?:bb).+$", cudf::strings::regex_flags::DEFAULT, cudf::strings::capture_groups::NON_CAPTURE);
        // std::unique_ptr<cudf::column> match_bools =
        //   cudf::strings::matches_re(result->get_column(0).view(), *reg_filter);
        // result = cudf::apply_boolean_mask(*result, *match_bools);

        // sort by sort_cols. if a timestamp column was provided, then sort that column descending.
        std::vector<cudf::size_type> sort_cols{ 0 };
        std::vector<cudf::order> sort_order(sort_cols.size(), cudf::order::ASCENDING);
        std::vector<cudf::null_order> null_precedence(sort_cols.size(), cudf::null_order::BEFORE);
        result = cudf::sort_by_key(result->view(), result->view().select(sort_cols), sort_order, null_precedence);
        std::cout << ff("sort by key\n");
        return { std::move(result), std::move(table_with_metadata.metadata) };
    } catch (std::bad_alloc const &e) {
        SPDLOG_ERROR(ff("Caught error {}", e.what()));
        return { nullptr, {} };
    }
}

std::size_t write_range_low_mem(CompactionInput const &details,
  scalar_ptr const &low,
  scalar_ptr const &high,
  std::unique_ptr<cudf::io::parquet_chunked_writer> &writer,
  std::shared_ptr<Aws::S3::S3Client> &s3client) {
    auto table = table_for_range(details, low, high);
    if (table.tbl.get() == nullptr)
        return 0;

    try {
        // now open output file
        if (writer.get() == nullptr) {
            auto tim = cudf::io::table_input_metadata{ table.metadata };
            writer = make_writer(details, std::move(tim), s3client);
        }

        SPDLOG_INFO(ff("Writing chunk {:Ld}", table.tbl->num_rows()));
        writer->write(table.tbl->view());
    } catch (std::exception const &e) {
        SPDLOG_ERROR(ff("Writing threw exception {}", e.what()));
        throw;
    }

    return static_cast<std::size_t>(table.tbl->num_rows());
}

int walk_schema(parquet::format::FileMetaData const &fmd,
  std::vector<col_schema> &flat_schema,
  int idx,
  int max_def_level,
  int max_rep_level) {
    if (idx >= 0 && idx < static_cast<int>(fmd.schema.size())) {
        parquet::format::SchemaElement const &se = fmd.schema[idx];
        if (se.repetition_type == parquet::format::FieldRepetitionType::OPTIONAL) {
            ++max_def_level;
        } else if (se.repetition_type == parquet::format::FieldRepetitionType::REPEATED) {
            ++max_def_level;
            ++max_rep_level;
        }

        ++idx;
        if (se.num_children > 0) {
            for (int i = 0; i < se.num_children; i++) {
                int const idx_old = idx;
                idx = walk_schema(fmd, flat_schema, idx, max_def_level, max_rep_level);
                if (idx <= idx_old) {
                    break;
                }// Error
            }
        } else {
            flat_schema.push_back({ se, max_def_level, max_rep_level });
        }
        return idx;
    } else {
        // Error
        return -1;
    }
}

std::optional<size_t> page_size(col_schema const &schema,
  parquet::format::OffsetIndex const &offidx,
  parquet::format::ColumnIndex const &colidx,
  int pg_idx,
  int num_pages,
  int num_rows_in_chunk,
  bool print) {

    auto const pg_start_row = offidx.page_locations[pg_idx].first_row_index;
    auto const pg_end_row =
      pg_idx == (num_pages - 1) ? num_rows_in_chunk : offidx.page_locations[pg_idx + 1].first_row_index;
    auto const num_rows = pg_end_row - pg_start_row;

    // Need to get the number of values and number of nulls for the page. First try def histograms,
    // and if they aren't available, then see if we can use num_rows from the column index.
    std::optional<int> num_values;// for fixed width, we leave space for null values
    std::optional<int> num_nulls;// for byte_array, we only need space for non-nulls

    // check for num_nulls from colidx
    if (colidx.__isset.null_counts) {
        num_nulls = colidx.null_counts[pg_idx];
    }

    if (colidx.__isset.definition_level_histograms) {
        auto hist = &colidx.definition_level_histograms[pg_idx * (schema.max_def + 1)];

        if (not num_nulls.has_value()) {
            for (int i = 0; i < schema.max_def; i++) { num_nulls = num_nulls.value() + hist[i]; }
        }
        num_values = num_nulls.value() + hist[schema.max_def];
    }
    // there is no def histogram.
    // if there is no repetition (no lists), then num_values == num_rows, and num_nulls can be
    // obtained from the column index
    else if (schema.max_rep == 0) {
        num_values = num_rows;

        // didn't get null info from the column index :(
        if (not num_nulls.has_value()) {
            if (schema.max_def == 0) {
                num_nulls = 0;
            }
        }
    }
    // if the rep level histogram is present, we can get the total number of values from that
    else if (colidx.__isset.repetition_level_histograms) {
        if (num_nulls.has_value()) {
            auto const h = &colidx.repetition_level_histograms[pg_idx * (schema.max_rep + 1)];
            num_values = std::reduce(h, h + schema.max_rep + 1);
        }
    }

    // not enough sizing info
    if (not num_nulls.has_value() or not num_values.has_value()) {
        return std::nullopt;
    }

    if (print) {
        std::cout << ff(
          "pg_idx {:Ld} pg_start_row {:Ld} pg_end_row {:Ld} num_rows {:Ld} num_values {:Ld} num_nulls {:Ld}\n",
          pg_idx,
          pg_start_row,
          pg_end_row,
          num_rows,
          *num_values,
          *num_nulls);
    }

    // for strings return unencoded_byte_array_data_bytes + num_vals * sizeof(int)
    if (schema.se.type == parquet::format::Type::BYTE_ARRAY) {
        size_t res = 0;
        if (offidx.__isset.unencoded_byte_array_data_bytes) {
            res = offidx.unencoded_byte_array_data_bytes[pg_idx];
            if (print) {
                std::cout << ff("Byte array size {:Ld}", res);
            }
        } else {
            if (print) {
                std::cout << "Byte array size unknown";
            }
        }
        res += (num_values.value() - num_nulls.value()) * sizeof(int);
        if (print) {
            std::cout << ff(" for a total size of {:Ld}\n", res);
        }
        return res;
    }

    // for fixed-width types, return num_values * data_size
    switch (schema.se.type) {
    case parquet::format::Type::BOOLEAN:
        return num_values.value();

    // TODO: check for logical types for int32
    case parquet::format::Type::INT32:
    case parquet::format::Type::FLOAT:
        return num_values.value() * sizeof(int);

    case parquet::format::Type::INT64:
    case parquet::format::Type::DOUBLE:
        return num_values.value() * sizeof(double);

    case parquet::format::Type::FIXED_LEN_BYTE_ARRAY:
        return schema.se.type_length;
    default:
        return std::nullopt;
    }
}

CompactionResult mergeSortedS3Files(CompactionInput const &details, std::size_t gpuChunk) {
    auto s3client = gpu_compact::s3::makeClient();
    // TODO this should be read from compaction input details, will consist of row keys and sort keys
    // get the column to use for ranges
    cudf::size_type const range_col = { 0 };

    // force gpu initialization so it's not included in the time
    rmm::cuda_stream_default.synchronize();

    // need to create table_input_metadata to get column names and nullability correct
    size_t num_columns = 0;
    size_t total_rows = 0;
    std::vector<std::vector<parquet::format::ColumnIndex>> indexes_per_file;

    std::vector<parquet::format::SchemaElement> schema;
    std::vector<col_schema> flat_schema;
    std::vector<page_info> pages;

    // collect per-page sizing info
    std::vector<std::string> const &inputFiles = details.inputFiles;
    for (size_t f = 0; f < inputFiles.size(); f++) {
        std::cout << "File " << f << "..." << std::flush;
        auto [fmeta, offset_index, column_index] = read_indexes(inputFiles[f]);
        // use first input file to get schema and column info
        if (f == 0) {
            schema = fmeta.schema;
            num_columns = fmeta.row_groups[0].columns.size();

            // flatten the schema so we can use it for size estimation later
            walk_schema(fmeta, flat_schema, 0, 0, 0);
        }

        int file_colidx = 0;
        for (int rg_idx = 0; rg_idx < static_cast<int>(fmeta.row_groups.size()); rg_idx++) {
            auto const &row_grp = fmeta.row_groups[rg_idx];
            total_rows += row_grp.num_rows;

            for (int col_idx = 0; col_idx < static_cast<int>(row_grp.columns.size()); col_idx++, file_colidx++) {
                auto const &offsets = offset_index[file_colidx];
                auto const &colidxs = column_index[file_colidx];
                int const global_col_idx = static_cast<int>(f * num_columns + col_idx);

                size_t const num_pages = offsets.page_locations.size();
                for (unsigned int pg_idx = 0; pg_idx < num_pages; pg_idx++) {
                    bool const print = (rg_idx == 0 && col_idx == 0 && pg_idx < 2);
                    auto const &page_loc = offsets.page_locations[pg_idx];
                    if (print) {
                        std::cout << "Row group " << rg_idx << " num rows " << row_grp.num_rows << std::endl;
                        std::cout << "col_idx " << col_idx << std::endl;
                    }
                    auto const page_sz = page_size(flat_schema[col_idx],
                      offsets,
                      colidxs,
                      pg_idx,
                      static_cast<int>(num_pages),
                      static_cast<int>(row_grp.num_rows),
                      print);
                    if (not page_sz.has_value()) {
                        SPDLOG_CRITICAL(ff("no sizing info"));
                        throw std::runtime_error("no sizing info");
                    }

                    int const num_rows = static_cast<int>(
                      pg_idx == num_pages - 1
                        ? row_grp.num_rows - page_loc.first_row_index
                        : offsets.page_locations[pg_idx + 1].first_row_index - page_loc.first_row_index);
                    page_info pp{ static_cast<int>(f),
                        rg_idx,
                        file_colidx,
                        static_cast<int>(pg_idx),
                        col_idx,
                        global_col_idx,
                        num_rows,
                        page_sz.value() };
                    pages.push_back(std::move(pp));
                }
            }
        }

        indexes_per_file.push_back(std::move(column_index));
    }
    std::cout << '\n';
    // get type for range_col
    int schema_idx = 0;
    int col_idx = -1;
    parquet::format::Type::type col_type = parquet::format::Type::BOOLEAN;
    parquet::format::LogicalType log_type;
    parquet::format::ConvertedType::type conv_type = parquet::format::ConvertedType::BSON;
    for (auto const &se : schema) {
        if (se.num_children == 0) {
            col_idx++;
            if (col_idx == range_col) {
                col_type = se.type;
                conv_type = se.converted_type;
                log_type = se.logicalType;
                break;
            }
        }
        schema_idx++;
    }

    cudf::logger().set_level(spdlog::level::debug);

    // chunk-size is in GB
    // TODO read from config/options
    size_t const chunk_size = gpuChunk * 1024ul * 1024ul;
    // calculate input ranges
    auto ranges = getRanges(pages, range_col, col_type, conv_type, chunk_size, indexes_per_file);

    // use pooled memory...speeds up mallocs for table copies
    auto mr = make_pooled_mr();

    // will be lazy initialized
    std::unique_ptr<cudf::io::parquet_chunked_writer> writer;

    auto tstart = timestamp();
    std::size_t count = 0;
    while (!ranges.empty()) {
        scalar_pair &curr = ranges.front();
        SPDLOG_INFO(ff("attempt range {} , {}", std::get<0>(curr), std::get<2>(curr)));
        try {
            count += write_range_low_mem(details, std::get<1>(curr), std::get<3>(curr), writer, s3client);
            ranges.pop_front();
        } catch (std::exception const &e) {
            SPDLOG_ERROR(ff("processing range {}, {} failed {}", std::get<0>(curr), std::get<2>(curr), e.what()));
            throw;
        }
    }

    writer->close();

    auto tend = timestamp();

    SPDLOG_INFO(ff("total num rows read {:Ld}", count));
    finalise_writer();
    using fseconds = std::chrono::duration<double, std::chrono::seconds::period>;
    SPDLOG_INFO(ff("total time {:L} seconds", std::chrono::duration_cast<fseconds>(tend - tstart).count()));
    return { count, count };
}

CompactionResult merge_sorted_files(CompactionInput const &details, std::size_t gpuChunk) {
    gpu_compact::s3::initialiseAWS();
    auto result = mergeSortedS3Files(details, gpuChunk);
    gpu_compact::s3::shutdownAWS();
    return result;
}

}// namespace gpu_compact::cudf_compact
