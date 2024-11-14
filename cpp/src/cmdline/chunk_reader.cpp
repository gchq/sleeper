#include "configure_logging.hpp"

#include <CLI/CLI.hpp>// NOLINT

#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/io/datasource.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/merge.hpp>
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

#include "data_sink.hpp"
#include "io/prefetch_source.hpp"
#include "io/s3_utils.hpp"
#include "lub.hpp"
#include "slice.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

::size_t calcRowsWritten(auto const &readers) noexcept {
    return std::accumulate(
      readers.cbegin(), readers.cend(), ::size_t{ 0 }, [](auto &&acc, auto const &item) constexpr noexcept {
          return acc + std::get<3>(item);
      });
}

[[nodiscard]] std::chrono::time_point<std::chrono::steady_clock> timestamp() noexcept {
    return std::chrono::steady_clock::now();
}

[[nodiscard]] cudf::io::table_metadata grabMetaData(std::string const &file) {
    auto opts = cudf::io::parquet_reader_options::builder(cudf::io::source_info(file)).num_rows(1).build();
    return cudf::io::read_parquet(opts).metadata;
}

int main(int argc, char **argv) {
    configure_logging();
    // NOLINTNEXTLINE
    CLI::App app{ "Simple program to test chunking compaction algorithm with cuDF", "chunk_reader" };
    app.set_version_flag("--version", std::string{ gpu_compact::cmake::project_version });

    std::string outputFile;
    app.add_option("output", outputFile, "Output path for Parquet file")->required();
    std::vector<std::string> inputFiles;
    app.add_option("input", inputFiles, "Input Parquet files")->required()->expected(1, -1);
    std::size_t chunkReadLimit{ 1024 };
    app.add_option("-c,--chunk-read-limit", chunkReadLimit, "cuDF Parquet reader chunk read limit in MiB");
    std::size_t passReadLimit{ 1024 };
    app.add_option("-p,--pass-read-limit", passReadLimit, "cuDF Parquet reader pass read limit in MiB");
    std::size_t epsilon{ 10'000 };
    app.add_option("-e,--epsilon", epsilon, "Lower bound for rows remaining in a table before loading next chunk");
    CLI11_PARSE(app, argc, argv);// NOLINT

    // force gpu initialization so it's not included in the time
    rmm::cuda_stream_default.synchronize();
    gpu_compact::s3::initialiseAWS();
    {
        auto s3client = gpu_compact::s3::makeClient();

        auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();
        auto mr =
          rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(cuda_mr, rmm::percent_of_free_device_memory(95));
        rmm::mr::set_current_device_resource(mr.get());

        // Container for all data sources and Parquet readers
        std::vector<std::tuple<std::unique_ptr<cudf::io::datasource>,
          std::unique_ptr<cudf::io::chunked_parquet_reader>,
          std::size_t,
          std::size_t>>
          readers;
        readers.reserve(inputFiles.size());

        // Make readers and find total row count
        // We create the data source and disable prefetching while we read the footer
        std::size_t totalRows = 0;
        for (auto const &f : inputFiles) {
            auto source =
              std::make_unique<gpu_compact::io::PrefetchingSource>(f, cudf::io::datasource::create(f), false);
            totalRows += cudf::io::read_parquet_metadata(cudf::io::source_info(source.get())).num_rows();
            auto reader_builder = cudf::io::parquet_reader_options::builder(cudf::io::source_info(&*source));
            readers.emplace_back(std::move(source),
              std::make_unique<cudf::io::chunked_parquet_reader>(
                chunkReadLimit * 1'048'576, passReadLimit * 1'048'576, reader_builder.build()),
              0,
              0);
            // Enable pre-fetching after footer read
            dynamic_cast<gpu_compact::io::PrefetchingSource *>(std::get<0>(readers.back()).get())->prefetch(true);
        }

        // Grab metadata for schema from first file
        auto const tableMetadata = grabMetaData(inputFiles[0]);
        // Make writer
        SinkInfoDetails sinkDetails = make_writer(outputFile, tableMetadata, s3client);
        auto &writer = *sinkDetails.writer;

        SPDLOG_INFO("Start reading files");
        // Remaining parts initially empty
        std::vector<std::unique_ptr<cudf::table>> remainingParts{ readers.size() };
        std::size_t lastTotalRowCount = std::numeric_limits<std::size_t>::max();
        auto const startTime = timestamp();
        // Loop doing reads
        while (lastTotalRowCount) {
            lastTotalRowCount = 0;
            // Loop through each reader
            for (std::size_t rc = 0; auto &[src, reader, chunkNo, rowCount] : readers) {
                // If reader has data and we need some, perform a read
                SPDLOG_INFO("Reader {:d}", rc);
                if (reader->has_next()) {
                    SPDLOG_INFO("   Reader has rows");
                    if (!remainingParts[rc] || remainingParts[rc]->num_rows() < epsilon) {
                        SPDLOG_INFO(
                          "    No previous table or we only have {:d} in memory", remainingParts[rc]->num_rows());

                        // Read a chunk
                        SPDLOG_INFO("    Read chunk: {:d}", chunkNo);
                        auto table = reader->read_chunk();
                        auto const rowsInChunk = table.metadata.num_rows_per_source.at(0);
                        SPDLOG_INFO("    Read chunk of {:d} rows", rowsInChunk);
                        // Increment chunk number in reader and add to row count
                        chunkNo++;
                        rowCount += rowsInChunk;

                        // Now concat the old part to the new chunk
                        std::unique_ptr<cudf::table> concat =
                          cudf::concatenate(std::vector{ remainingParts[rc]->view(), table.tbl->view() });
                        remainingParts[rc] = std::move(concat);
                        SPDLOG_INFO("    New table has {:d} rows", remainingParts[rc]->num_rows());
                    }
                } else {
                    SPDLOG_INFO("    Reader {:d} has no more rows", rc);
                }

                // Update overall count
                lastTotalRowCount += remainingParts[rc]->num_rows();
                rc++;
            }

            // Merge and write tables
            if (lastTotalRowCount > 0) {
                // Find the least upper bound in sort column across these tables
                auto const leastUpperBound = findLeastUpperBound(remainingParts, 0);

                // Now take search "needle" from last row from of table with LUB
                auto const lubTable = remainingParts[leastUpperBound]->select({ 0 });
                auto const needle = cudf::split(lubTable, { lubTable.num_rows() - 1 })[1];

                // Split all tables at the needle
                std::pair<std::vector<cudf::table_view>, std::vector<cudf::table_view>> const tableVectors =
                  splitAtNeedle(needle, remainingParts);

                // Merge all the upper parts of the tables
                SPDLOG_INFO("Merging {:d} rows", lastTotalRowCount);
                auto merged = cudf::merge(tableVectors.first, { 0 }, { cudf::order::ASCENDING });

                // Duplicate the unmerged parts of the tables, so we can opportunistically clear the original
                // tables we no longer need
                for (std::size_t idx = 0; auto &&table : remainingParts) {
                    table = std::make_unique<cudf::table>(tableVectors.second[idx]);
                    idx++;
                }

                writer.write(*merged);

                auto const elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(timestamp() - startTime);
                auto const rowsWritten = calcRowsWritten(readers);
                auto const fracRowsWritten = (static_cast<double>(rowsWritten) / totalRows);
                auto const predictedTime =
                  std::chrono::duration_cast<std::chrono::seconds>(elapsedTime * (1 / fracRowsWritten));
                SPDLOG_INFO("Written {:d} rows, {:.2f}% complete, est. time (total) {:02d}:{:02d} ({:02d}:{:02d})",
                  rowsWritten,
                  fracRowsWritten * 100,
                  elapsedTime.count() / 60,
                  elapsedTime.count() % 60,
                  predictedTime.count() / 60,
                  predictedTime.count() % 60);
            }
        }

        writer.close();

        // Grab total row count from each reader
        auto const rowsWritten = calcRowsWritten(readers);

        SPDLOG_INFO("Finished, read/wrote {:d} rows from {:d} readers", rowsWritten, inputFiles.size());
    }
    gpu_compact::s3::shutdownAWS();
}
