#include "configure_logging.hpp"

#include <CLI/CLI.hpp>// NOLINT

#include <cudf/concatenate.hpp>
#include <cudf/io/datasource.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/sorting.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/error.hpp>
#include <internal_use_only/config.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/owning_wrapper.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>
#ifdef SPDLOG_ACTIVE_LEVEL
#undef SPDLOG_ACTIVE_LEVEL
#endif
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <cstddef>
#include <memory>
#include <string>

int main(int argc, char **argv) {
    configure_logging();
    // NOLINTNEXTLINE
    CLI::App app{ "Simple program to check if Parquet file is sorted with cuDF", "check_sort" };
    app.set_version_flag("--version", std::string{ gpu_compact::cmake::project_version });

    std::string inputFile;
    app.add_option("input", inputFile, "Input Parquet file")->required();
    std::string colName;
    app.add_option("Column name", colName, "Column to validate sort order")->required();
    CLI11_PARSE(app, argc, argv);// NOLINT

    // force gpu initialization so it's not included in the time
    rmm::cuda_stream_default.synchronize();

    auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();
    auto mr =
      rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(cuda_mr, rmm::percent_of_free_device_memory(95));
    rmm::mr::set_current_device_resource(mr.get());

    auto opts =
      cudf::io::parquet_reader_options::builder(cudf::io::source_info(inputFile)).columns({ colName }).build();
    cudf::io::chunked_parquet_reader reader{ 500 * 1'048'576l, 500 * 1'048'576l, opts };

    SPDLOG_INFO("Validating sort on column '{}' in file '{}'", colName, inputFile);

    // Loop doing reads
    // A sort problem may occur across a chunk boundary, so we use a slightly hacky workaround of keeping
    // the last chunk stored and then concatenate the current chunk on to it and then check that for sorting.
    ::size_t totalRowsRead = 0;
    ::size_t chunkNo = 0;
    cudf::io::table_with_metadata prevTable;
    while (reader.has_next()) {
        auto currentTable = reader.read_chunk();
        ::size_t rowsRead = currentTable.tbl->num_rows();
        SPDLOG_INFO("Checking chunk number {:d} has {:d} rows", chunkNo, rowsRead);

        std::unique_ptr<cudf::table> checkTable;
        cudf::table_view checkView = currentTable.tbl->view();
        if (prevTable.tbl) {
            checkTable = cudf::concatenate(std::vector<cudf::table_view>{ *prevTable.tbl, *currentTable.tbl });
            checkView = std::move(checkTable->view());
        }
        bool chunkSorted = cudf::is_sorted(checkView, { cudf::order::ASCENDING }, { cudf::null_order::AFTER });
        if (!chunkSorted) {
            SPDLOG_ERROR("Chunk number {:d} contains an incorrect sort order between rows [{:d},{:d})",
              chunkNo,
              totalRowsRead,
              totalRowsRead + rowsRead);
            CUDF_FAIL("Incorrect sort detected");
        }
        chunkNo++;
        totalRowsRead += rowsRead;
        prevTable = std::move(currentTable);
    }

    SPDLOG_INFO("Finished, file is correctly sorted");
}
