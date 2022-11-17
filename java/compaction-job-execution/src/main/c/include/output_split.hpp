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

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "histo.hpp"

template <typename To, typename From>
constexpr bool checkBounds(From const& value) noexcept {
    return (!(value < std::numeric_limits<To>::lowest()) && (value <= std::numeric_limits<To>::max()));
}

template <typename T>
constexpr bool false_constant = std::false_type::value;

template <typename To, typename T>
constexpr To checkAndCast(T const& value) noexcept(false) {
    if (checkBounds<To>(value)) {
        return static_cast<To>(value);
    } else {
        throw std::out_of_range("value is out of range of target type!!!!");
    }
}

template <typename T>
T convert(std::string const& line) noexcept(false) {
    if constexpr (std::is_same_v<std::string, T>) {
        return std::move(line);
    } else if constexpr (std::is_convertible_v<std::string, T>) {
        return T{line};
    } else if constexpr (std::is_integral_v<T>) {
        if constexpr (std::is_signed_v<T>) {
            long long result = std::stoll(line);
            return checkAndCast<T>(result);
        } else {  // unsigned
            unsigned long long result = std::stoull(line);
            return checkAndCast<T>(result);
        }
    } else {
        static_assert(false_constant<T>, "Don't have built in conversion from string for this type");
    }
}

template <typename T = std::string>
std::vector<T> convertVector(std::vector<std::string> const& input) {
    std::vector<T> result;
    result.reserve(input.size());
    for (auto const& item : input) {
        result.push_back(convert<T>(item));
    }
    return result;
}

std::vector<std::string> loadOutputPartsFile(std::filesystem::path const& path);

template <typename T>
void insertToEdgeList(std::vector<EdgePair<T>>& edgeList, std::vector<T> const& outputSplits) {
    static constexpr std::less less_comp{};
    static constexpr std::greater greater_comp{};
    if (!std::is_sorted(outputSplits.cbegin(), outputSplits.cend())) {
        throw std::invalid_argument("output split point list not in ascending order!");
    }
    if (edgeList.empty()) {
        throw std::invalid_argument("edge list is empty");
    }
    // iterate over the output split points.
    for (auto const& splitPoint : outputSplits) {
        // is it before the beginning or after the end?
        if (less_comp(splitPoint, edgeList.front().first) || greater_comp(splitPoint, edgeList.back().second)) {
            continue;
        }
        // locate where this belongs in the edge list (according to first entry of EdgePair). This always
        // returns one passed where we need to we jump to the previous element
        typename std::decay_t<decltype(edgeList)>::iterator pos = std::lower_bound(
            edgeList.begin(), edgeList.end(), splitPoint,
            [&](EdgePair<T> const& edge, T const& split) {
                return edge.first <= split;
            });
        // step back
        pos = std::prev(pos);
        // do we need to modify anything? If this split point exactly matches one of the elements of the edge
        // then skip
        if (pos->first == splitPoint || pos->second == splitPoint) {
            continue;
        }
        // modify the edge list by splitting the edge pair in two
        // first stash the second part of the edge pair we are splitting
        T tempEdgeEnd{std::move(pos->second)};
        // set the new end
        pos->second = splitPoint;
        // insert the new split
        edgeList.emplace(std::next(pos), splitPoint, std::move(tempEdgeEnd));
    }
}
