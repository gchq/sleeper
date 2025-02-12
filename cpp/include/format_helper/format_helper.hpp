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

#include <fmt/core.h>
#include <mutex>
#include <utility>

void switchLocale() noexcept;

std::lock_guard<std::mutex> lockGuard() noexcept;

template<typename Msg, typename... Args> inline auto ff(Msg &&msg, Args &&...args) noexcept {
    auto const guard = lockGuard();
    switchLocale();
    auto const ret = fmt::vformat(std::forward<Msg>(msg), fmt::make_format_args(std::forward<Args>(args)...));
    switchLocale();
    return ret;
}
