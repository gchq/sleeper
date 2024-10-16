#include "configure_logging.hpp"

#include <CLI/CLI.hpp>// NOLINT
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
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
    CLI::App app{ "Simple program to test spooling through Parquet files in chunks with cuDF", "chunk_reader" };
    app.set_version_flag("--version", std::string{ gpu_compact::cmake::project_version });

    std::string outputFile;
    app.add_option("output", outputFile, "Output path for Parquet file")->required();
    std::string inputFile;
    app.add_option("input", inputFile, "Input Parquet file")->required();
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

    // Make reader
    auto source = cudf::io::source_info(inputFile);
    auto reader_builder = cudf::io::parquet_reader_options::builder(source);
    auto reader = cudf::io::chunked_parquet_reader{
        chunkReadLimit * 1'048'576,
        passReadLimit * 1'048'576,
        reader_builder.build(),
    };

    // Make writer
    auto sink = cudf::io::sink_info(outputFile);
    auto writer_builder = cudf::io::chunked_parquet_writer_options::builder(sink);
    auto writer = cudf::io::parquet_chunked_writer{ writer_builder.build() };

    // Loop doing reads
    ::size_t chunkNo = 0;
    while (reader.has_next()) {
        SPDLOG_INFO("Reading chunk {:d}...", chunkNo);
        auto const table = reader.read_chunk();
        SPDLOG_INFO("Read chunk of {:d} rows", table.metadata.num_rows_per_source.at(0));
        std::cout << "Write chunk ";
        for (::size_t i = 0; i < writeRepeats; i++) {
            std::cout << i << ", " << std::flush;
            writer.write(*table.tbl);
        }
        std::cout << '\n';
        SPDLOG_INFO("Wrote chunk {:d}", chunkNo);
        chunkNo++;
    }

    writer.close();
}