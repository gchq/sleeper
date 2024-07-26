#include <CLI/CLI.hpp>// NOLINT
#include <ProtoCompaction.pb.h>
#include <cstdlib>
#include <exception>
#include <fmt/core.h>
#include <grpcpp/grpcpp.h>
#include <lefticus/tools/non_promoting_ints.hpp>// NOLINT
#include <optional>
#include <spdlog/spdlog.h>
#include <string>

#include <gpu_compact/sample_library.hpp>

// This file will be generated automatically when cur_you run the CMake
// configuration step. It creates a namespace called `gpu_compact`. You can modify
// the source template at `configured_files/config.hpp.in`.
#include <internal_use_only/config.hpp>

using sleeper::compaction::job::execution::CompactionService;
using sleeper::compaction::job::execution::CompactionParams;
using sleeper::compaction::job::execution::CompactionResult;
using grpc::Status;
struct CompactionServer : public CompactionService
{
    [[nodiscard]] grpc::Status
      compact(grpc::ServerContext *const context, CompactionParams const *params, CompactionResult result)
    {
        return Status::OK;
    }
};

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, const char **argv)
{
    try {
        // NOLINTNEXTLINE
        CLI::App app{ fmt::format(
          "{} version {}", gpu_compact::cmake::project_name, gpu_compact::cmake::project_version) };

        std::optional<std::string> message;
        app.add_option("-m,--message", message, "A message to print back out");
        bool show_version = false;// NOLINT(misc-const-correctness)
        app.add_flag("--version", show_version, "Show version information");

        CLI11_PARSE(app, argc, argv);// NOLINT

        if (show_version) {
            fmt::print("{}\n", gpu_compact::cmake::project_version);
            return EXIT_SUCCESS;
        }

        fmt::print("Factorial of {} is {}\n", 2, factorial(2));


    } catch (const std::exception &e) {
        spdlog::error("Unhandled exception in main: {}", e.what());
    }
}
