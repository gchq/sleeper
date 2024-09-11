#include "configure_logging.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

void configure_logging() noexcept {
    auto colour_logger = spdlog::stdout_color_st("console");
    spdlog::set_default_logger(colour_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S %^[%l]%$ %s:%# - %v");
}
