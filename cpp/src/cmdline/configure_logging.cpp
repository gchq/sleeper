/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "configure_logging.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

void configure_logging() noexcept {
    auto colour_logger = spdlog::stdout_color_st("console");
    spdlog::set_default_logger(colour_logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("%Y-%m-%dT%H:%M:%S %^[%l]%$ %s:%# - %v");
}
