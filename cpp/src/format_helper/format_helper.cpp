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
#include "format_helper/format_helper.hpp"

#include <iostream>
#include <locale>
#include <string>

class thousands_sep : public std::numpunct<char>
{
    char do_thousands_sep() const noexcept override {
        return ',';
    }
    std::string do_grouping() const noexcept override {
        return "\3";
    }
};

std::lock_guard<std::mutex> lockGuard() noexcept {
    static std::mutex lock;
    return std::lock_guard{ lock };
}

// NOT thread safe!
void switchLocale() noexcept {
    static thousands_sep punct{};
    static std::locale spare{ std::locale(), &punct };
    // swap the global locale
    spare = std::locale::global(spare);
}
