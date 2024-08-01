#include <CLI/CLI.hpp>// NOLINT
#include <ProtoCompaction.grpc.pb.h>
#include <ProtoCompaction.pb.h>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fmt/core.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <lefticus/tools/non_promoting_ints.hpp>// NOLINT
#include <limits>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>

#include <gpu_compact/sample_library.hpp>

// This file will be generated automatically when cur_you run the CMake
// configuration step. It creates a namespace called `gpu_compact`. You can modify
// the source template at `configured_files/config.hpp.in`.
#include <internal_use_only/config.hpp>

using sleeper::compaction::job::execution::Compactor;
using sleeper::compaction::job::execution::CompactionParams;
using sleeper::compaction::job::execution::CompactionResult;
using sleeper::compaction::job::execution::ReturnCode;
using grpc::Status;
using grpc::ServerBuilder;

class CompactionService final : public Compactor::Service
{
    grpc::Status compact([[maybe_unused]] ::grpc::ServerContext *context,
      const ::sleeper::compaction::job::execution::CompactionParams *params,
      ::sleeper::compaction::job::execution::CompactionResult *result) noexcept override
    {
        std::cout << params->output_file() << " " << params->dict_enc_values() << '\n';
        std::uint64_t test = std::numeric_limits<std::uint64_t>::max() - 2;
        ReturnCode rc = ReturnCode::FAIL;
        result->set_exit_status(rc);
        result->set_rows_read(test);
        result->set_rows_written(342);
        result->set_msg("exit msg test");
        return Status::OK;
    }
};

// NOLINTNEXTLINE(bugprone-exception-escape)
int main([[maybe_unused]] int argc, [[maybe_unused]] const char **argv)
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

        CompactionService compactor{};
        ServerBuilder builder{};
        builder.AddListeningPort("[::]:5678", grpc::InsecureServerCredentials());
        builder.RegisterService(&compactor);
        std::unique_ptr server = builder.BuildAndStart();
        server->Wait();
    } catch (const std::exception &e) {
        spdlog::error("Unhandled exception in main: {}", e.what());
    }
}
