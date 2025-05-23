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
#pragma once
/**
 * @file common_types.hpp
 * @brief Common type declarations.
 * @date 2023
 *
 * @copyright Copyright 2022-2025 Crown Copyright
 *
 */
#include <cstdint>
#include <vector>
/**
 * @brief Standard way of representing byte arrays in a way that is easily compatible with Rust.
 */
using byte_array = std::vector<::uint8_t>;
