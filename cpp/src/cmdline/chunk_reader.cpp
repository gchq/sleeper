#include "configure_logging.hpp"

#include <CLI/CLI.hpp>// NOLINT
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/merge.hpp>
#include <cudf/types.hpp>
#include <internal_use_only/config.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/mr/device/owning_wrapper.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>
#ifdef SPDLOG_ACTIVE_LEVEL
#undef SPDLOG_ACTIVE_LEVEL
#endif
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

int main(int argc, char **argv) {
    configure_logging();
    // NOLINTNEXTLINE
    CLI::App app{ "Simple program to test spooling through Parquet files in chunks with cuDF", "chunk_reader" };
    app.set_version_flag("--version", std::string{ gpu_compact::cmake::project_version });

    std::string outputFile;
    app.add_option("output", outputFile, "Output path for Parquet file")->required();
    std::vector<std::string> inputFiles;
    app.add_option("input", inputFiles, "Input Parquet files")->required()->expected(1, -1);
    std::size_t chunkReadLimit{ 1000 };
    app.add_option("-c,--chunk-read-limit", chunkReadLimit, "cuDF Parquet reader chunk read limit in MiB");
    std::size_t passReadLimit{ 1000 };
    app.add_option("-p,--pass-read-limit", passReadLimit, "cuDF Parquet reader pass read limit in MiB");
    std::size_t writeRepeats{ 1 };
    app.add_option("-w,--write-repeats", writeRepeats, "Number of times to repeat chunk");
    CLI11_PARSE(app, argc, argv);// NOLINT

    // force gpu initialization so it's not included in the time
    rmm::cuda_stream_default.synchronize();

    auto cuda_mr = std::make_shared<rmm::mr::cuda_memory_resource>();
    auto mr =
      rmm::mr::make_owning_wrapper<rmm::mr::pool_memory_resource>(cuda_mr, rmm::percent_of_free_device_memory(95));
    rmm::mr::set_current_device_resource(mr.get());

    // Make readers
    std::vector<std::tuple<std::unique_ptr<cudf::io::chunked_parquet_reader>, ::size_t, ::size_t>> readers;
    readers.reserve(inputFiles.size());

    for (auto const &f : inputFiles) {
        auto reader_builder = cudf::io::parquet_reader_options::builder(cudf::io::source_info(f));
        readers.emplace_back(std::make_unique<cudf::io::chunked_parquet_reader>(
                               chunkReadLimit * 1'048'576, passReadLimit * 1'048'576, reader_builder.build()),
          0,
          0);
    }

    // Make writer
    auto sink = cudf::io::sink_info(outputFile);
    auto writer_builder = cudf::io::chunked_parquet_writer_options::builder(sink);
    auto writer = cudf::io::parquet_chunked_writer{ writer_builder.build() };

    // Loop doing reads
    ::size_t lastTotalRowCount = std::numeric_limits<::size_t>::max();
    while (lastTotalRowCount) {
        lastTotalRowCount = 0;
        // Loop through each reader
        std::vector<cudf::io::table_with_metadata> tables;

        for (::size_t rc = 0; auto &[reader, chunkNo, rowCount] : readers) {
            // If reader has data, read a chunk and write, otherwise flag and ignore
            SPDLOG_INFO("Reader {:d}", rc);
            if (reader->has_next()) {
                SPDLOG_INFO("    Chunk: {:d}", chunkNo);
                tables.push_back(reader->read_chunk());
                auto const rowsInChunk = tables.back().metadata.num_rows_per_source.at(0);
                SPDLOG_INFO("    Read chunk of {:d} rows", rowsInChunk);

                // Increment chunk number in reader and add to row count
                chunkNo++;
                rowCount += rowsInChunk;

                // Update overall count
                lastTotalRowCount += rowsInChunk;
            } else {
                SPDLOG_INFO("    Reader {:d} has no more rows", rc);
            }
            rc++;
        }

        // Merge and write tables
        if (lastTotalRowCount) {
            std::vector<cudf::table_view> views;
            views.reserve(tables.size());
            for (auto const &table : tables) { views.push_back(*table.tbl); }
            SPDLOG_INFO("Merging {:d} rows", lastTotalRowCount);
            auto merged = cudf::merge(views, { 0 }, { cudf::order::ASCENDING });
            SPDLOG_INFO("Writing rows");
            writer.write(*merged);
        }
    }

    writer.close();

    // Grab total row count from each reader
    auto const totalRows = std::accumulate(
      readers.cbegin(), readers.cend(), ::size_t{ 0 }, [](auto &&acc, auto const &item) constexpr noexcept {
          return acc + std::get<2>(item);
      });

    SPDLOG_INFO("Finished, read/wrote {:d} rows from {:d} readers", totalRows, inputFiles.size());
}