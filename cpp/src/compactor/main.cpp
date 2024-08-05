#include <CLI/CLI.hpp>// NOLINT
#include <exception>
// #include <lefticus/tools/non_promoting_ints.hpp>// NOLINT
#include <regex>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

// This file will be generated automatically when cur_you run the CMake
// configuration step. It creates a namespace called `gpu_compact`. You can modify
// the source template at `configured_files/config.hpp.in`.
#include <internal_use_only/config.hpp>

static const std::regex URL_CHECK(R"((file|s3)://(/?(-.)?([^\s/?.#-]+.?)+(/[^\s]*)?))");

void configure_logging() noexcept
{
    auto colour_logger = spdlog::stdout_color_st("console");
    spdlog::set_default_logger(colour_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S %^[%l]%$ %s:%# - %v");
}

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, const char **argv)
{
    configure_logging();

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

        for (auto &url : inputFiles) {
            if (!std::regex_match(url, URL_CHECK)) {
                url = "file://" + url;
                if (!std::regex_match(url, URL_CHECK)) {
                    SPDLOG_ERROR("{} is not valid", url);
                    continue;
                }
            }
            SPDLOG_INFO("{} is valid", url);
        }
    } catch (std::exception const &e) {
        spdlog::error("Unhandled exception in main: {}", e.what());
    }
}
