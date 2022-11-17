/*
 * Copyright 2022 Crown Copyright
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
#include <string>
#include <algorithm>
#include <cctype>

template <typename CharT1, typename Traits1, typename Allocator1,
          typename CharT2, typename Traits2, typename Allocator2>
inline constexpr bool endsWith(std::basic_string<CharT1, Traits1, Allocator1> const& str,
                               std::basic_string<CharT2, Traits2, Allocator2> const& suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size() - suffix.size(), suffix.size(), suffix);
}

template <typename CharT1, typename Traits1, typename Allocator1,
          typename CharT2, typename Traits2, typename Allocator2>
inline constexpr bool startsWith(std::basic_string<CharT1, Traits1, Allocator1> const& str,
                                 std::basic_string<CharT2, Traits2, Allocator2> const& prefix) {
    return str.size() >= prefix.size() && 0 == str.compare(0, prefix.size(), prefix);
}

template <typename CharT1, typename Traits1, typename Allocator1,
          typename CharT2, typename Traits2, typename Allocator2>
bool iequals(std::basic_string<CharT1, Traits1, Allocator1> const & a,
std::basic_string<CharT2, Traits2, Allocator2> const & b)
{
    return std::equal(a.begin(), a.end(),
                      b.begin(), b.end(),
                      [](unsigned char a, unsigned char b) {
                          return std::tolower(a) == std::tolower(b);
                      });
}
