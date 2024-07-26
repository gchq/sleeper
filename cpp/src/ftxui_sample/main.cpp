#include <CLI/CLI.hpp>// NOLINT
#include <cstdlib>
#include <exception>
#include <fmt/core.h>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>

#include <lefticus/tools/non_promoting_ints.hpp>// NOLINT

#include <gpu_compact/sample_library.hpp>

// This file will be generated automatically when cur_you run the CMake
// configuration step. It creates a namespace called `gpu_compact`. You can modify
// the source template at `configured_files/config.hpp.in`.
#include <internal_use_only/config.hpp>


// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, const char **argv)
{
    try {
        // NOLINTNEXTLINE
        CLI::App app{ fmt::format(
          "{} version {}", gpu_compact::cmake::project_name, gpu_compact::cmake::project_version) };

        std::optional<std::string> message;
        app.add_option("-m,--message", message, "A message to print back out");
        bool show_version = false;
        app.add_flag("--version", show_version, "Show version information");

        bool is_turn_based = false;
        app.add_flag("--turn_based", is_turn_based);

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
