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
