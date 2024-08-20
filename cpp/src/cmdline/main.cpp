#include "configure_logging.hpp"
#include "cudf_compact/cudf_compact.hpp"
#include "locale_set.hpp"

#ifdef SPDLOG_ACTIVE_LEVEL
#undef SPDLOG_ACTIVE_LEVEL
#endif
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#include <CLI/CLI.hpp>// NOLINT
#include <spdlog/spdlog.h>
// #include <lefticus/tools/non_promoting_ints.hpp>// NOLINT

#include <cstdlib>
#include <exception>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>


using gpu_compact::cudf_compact::PartitionBound;
using gpu_compact::cudf_compact::ColRange;
using gpu_compact::cudf_compact::CompactionInput;
using gpu_compact::cudf_compact::CompactionResult;

// This file will be generated automatically when cur_you run the CMake
// configuration step. It creates a namespace called `gpu_compact`. You can modify
// the source template at `configured_files/config.hpp.in`.
#include <internal_use_only/config.hpp>

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, const char **argv) {
    configure_logging();
    configure_locale(std::cout);
    try {
        // NOLINTNEXTLINE
        CLI::App app{ fmt::format("{} version {} git SHA {}",
                        gpu_compact::cmake::project_name,
                        gpu_compact::cmake::project_version,
                        gpu_compact::cmake::git_sha),
            std::string{ gpu_compact::cmake::project_name } };
        app.set_version_flag("--version", std::string{ gpu_compact::cmake::project_version });

        std::string outputFile;
        app.add_option("output", outputFile, "Output file URL for the compacted Parquet data")->required();
        std::size_t rowGroupSize{ 1000000 };
        app.add_option("-r,--row-group-size", rowGroupSize, "Set the maximum number of rows in a row group");
        std::size_t maxPageSize{ 65535 };
        app.add_option("-p,--max-page-size", maxPageSize, "Set the maximum number of bytes per data page (hint)");
        std::vector<std::string> inputFiles;
        app.add_option("input", inputFiles, "List of input Parquet files (must be sorted) as URLs")
          ->required()
          ->expected(1, -1);
        std::vector<std::string> rowKeys;
        app.add_option("-k,--row-keys", rowKeys, "Column names for a row key column")->required()->expected(1, -1);
        std::vector<std::string> sortKeys;
        app.add_option("-s,--sort-keys", sortKeys, "Column names for sort key columns");
        std::vector<std::string> regionMins;
        app
          .add_option("--region-mins,-m",
            regionMins,
            "Partition region minimum keys (inclusive). Must be one per row key specified.")
          ->required()
          ->expected(1, -1);
        std::vector<std::string> regionMaxs;
        app
          .add_option("--region-maxs,-n",
            regionMaxs,
            "Partition region maximum keys (exclusive). Must be one per row key specified.")
          ->required()
          ->expected(1, -1);
        CLI11_PARSE(app, argc, argv);// NOLINT

        auto urlCheck = [](auto &s) noexcept {
            if (!std::regex_match(s, gpu_compact::cudf_compact::URL_CHECK)) {
                s = "file://" + s;
                if (!std::regex_match(s, gpu_compact::cudf_compact::URL_CHECK)) {
                    SPDLOG_ERROR("{} is not a valid URL", s);
                    return false;
                }
            }
            return true;
        };

        // Check to see if input urls are valid
        // for (auto &f : inputFiles) {
        //     if (!urlCheck(f)) { return EXIT_FAILURE; }
        // }

        // Check output URL
        if (!urlCheck(outputFile)) { return EXIT_FAILURE; }

        // Check lengths of vectors
        if (rowKeys.size() != regionMaxs.size()) {
            SPDLOG_ERROR("Must have same number of region maximums as row keys");
            return EXIT_FAILURE;
        }
        if (rowKeys.size() != regionMins.size()) {
            SPDLOG_ERROR("Must have same number of region minimums as row keys");
            return EXIT_FAILURE;
        }
        std::unordered_map<std::string, ColRange> region;
        for (auto minIter = regionMins.cbegin(), maxIter = regionMaxs.cbegin(); auto const &rowKey : rowKeys) {
            region.emplace(rowKey,
              ColRange{
                .lower = { *minIter++ }, .lower_inclusive = true, .upper = { *maxIter++ }, .upper_inclusive = false });
        }

        CompactionInput details{ .inputFiles = std::move(inputFiles),
            .outputFile = std::move(outputFile),
            .rowKeyCols = std::move(rowKeys),
            .sortKeyCols = std::move(sortKeys),
            .maxRowGroupSize = rowGroupSize,
            .maxPageSize = maxPageSize,
            .compression = "zstd",
            .writerVersion = "v2",
            .columnTruncateLength = 1'048'576,
            .statsTruncateLength = 1'048'576,
            .dictEncRowKeys = true,
            .dictEncSortKeys = true,
            .dictEncValues = true,
            .region = std::move(region) };

        auto [rowsRead, rowsWritten] = gpu_compact::cudf_compact::merge_sorted_files(details);

        SPDLOG_INFO("Compaction finished, rows read = {:Ld}, rows written = {:Ld}", rowsRead, rowsWritten);
    } catch (std::exception const &e) { SPDLOG_ERROR("Unhandled exception in main: {}", e.what()); }
}
