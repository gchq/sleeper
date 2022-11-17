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
#include <gmp.h>

#include <array>
#include <cmath>
#include <cstddef>
#include <functional>  //std::less
#include <numeric>     // std::accumulate
#include <ostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>  //std::pair
#include <vector>

template <typename T>
constexpr bool is_signed_integral_v = std::is_integral_v<T>&& std::is_signed_v<T>;

// Define what the character type should be
using char_type = std::string::value_type;

constexpr char_type ALPHA_MIN = '\x20';
constexpr char_type ALPHA_MAX = '\x7e';
static_assert(std::less{}(ALPHA_MIN, ALPHA_MAX), "Alpha min must be ordered before alpha max.");

template <char_type MIN = ALPHA_MIN, char_type MAX = ALPHA_MAX>
static constexpr auto const makeAlphabet() {
    std::array<char_type, static_cast<::size_t>(MAX - MIN) + 1> result{};
    char_type i = MIN;
    for (auto& c : result) {
        c = i++;
    }
    return result;
}

template <typename T>
struct Edge {
    using type = T;
    T label;
    ::size_t amount;
};

template <typename T>
using EdgePair = std::pair<T, T>;

template <typename T>
using Edge_vec = std::vector<Edge<T>>;

template <typename T>
using EdgePair_vec = std::vector<EdgePair<T>>;

template <typename T>
inline std::ostream& operator<<(std::ostream& out, Edge<T> const& edge) {
    return out << "{\"" << edge.label << "\", " << edge.amount << "}";
}

template <typename T>
inline std::ostream& operator<<(std::ostream& out, EdgePair<T> const& edge) {
    return out << "{\"" << edge.first << "\",\"" << edge.second << "\"}";
}

// Build alphabet at compile time, type of array deduced CTAD
inline constexpr const std::array ALPHABET = makeAlphabet();

inline constexpr const ::size_t ALPHABET_LENGTH = ALPHABET.size();

template <typename T, std::enable_if_t<is_signed_integral_v<std::decay_t<T>>, bool> = true>
std::decay_t<T> enbase(mpz_t const input) {
    // will the input fit in the given type?
    ::size_t bits = mpz_sizeinbase(input, 2);
    if (bits > (sizeof(T) * 8)) {
        throw std::invalid_argument("value too big to fit in given type");
    }
    std::decay_t<T> result = 0;
    mpz_export(&result, 0, -1, sizeof(result), 0, 0, input);
    return result;
}

template <typename T, std::enable_if_t<std::is_convertible_v<T, std::string>, bool> = true>
std::decay_t<T> enbase(mpz_t const input) {
    std::string result{};
    mpz_t mutated;
    mpz_t modulo;
    mpz_init(modulo);
    mpz_init_set(mutated, input);

    while (mpz_cmp_ui(mutated, ALPHABET_LENGTH) >= 0) {
        ::size_t rem = mpz_fdiv_ui(mutated, ALPHABET_LENGTH);
        result.insert(result.cbegin(), ALPHABET[rem]);
        mpz_fdiv_q_ui(mutated, mutated, ALPHABET_LENGTH);
    }
    ::size_t final_part = mpz_get_ui(mutated);
    result.insert(result.cbegin(), ALPHABET[final_part]);
    return result;
}

// only enable this template on signed arithmetic types
template <typename T, std::enable_if_t<is_signed_integral_v<std::decay_t<T>>, bool> = true>
void debase(mpz_t ret, T input) {
    mpz_set_si(ret, input);
}

void debase(mpz_t ret, std::string const & input);

template <typename T>
std::decay_t<T> midStr(T&& a, T&& b, double const frac) {
    using b_type = std::decay_t<T>;
    mpz_t left_d, right_d;
    mpz_inits(left_d, right_d, NULL);
    if constexpr (is_signed_integral_v<b_type>) {  // deal with integral types
        debase(left_d, a);
        debase(right_d, b);
    } else {  // strings
        ::size_t maxlen = std::max(a.size(), b.size());
        std::string left{a};
        std::string right{b};
        // fill out the strings to the maximum length
        left.resize(maxlen, ALPHABET.front());
        right.resize(maxlen, ALPHABET.back());
        // compute the integer representations of these strings
        debase(left_d, left);
        debase(right_d, right);
    }
    // compute mid point = left_d + ((right_d - left_d) * frac)
    mpz_sub(right_d, right_d, left_d);  // right_d = right_d - left_d
    // how much precision do we need to represent this?
    ::size_t prec = mpz_sizeinbase(right_d, 2);
    mpf_t frac_amount, proportion;
    mpf_init_set_d(frac_amount, frac);  // init and set to frac
    mpf_init2(proportion, prec);
    mpf_set_z(proportion, right_d);  // set proportion to right_d
    mpf_mul(proportion, proportion, frac_amount);
    mpz_set_f(right_d, proportion);     // convert to integer in right_d
    mpz_add(right_d, left_d, right_d);  // add left_d and store in right_d
                                        // convert this back to string / integral type
    b_type result = enbase<b_type>(right_d);
    mpz_clears(left_d, right_d, NULL);
    mpf_clears(frac_amount, proportion, NULL);
    return result;
}

template <typename T>
double findStrProp(T&& a, T&& b, T&& mid) {
    mpz_t r1, r2, r3;
    mpz_inits(r1, r2, r3, NULL);
    // compute integer repr. of left and middle
    if constexpr (is_signed_integral_v<std::decay_t<T>>) {
        debase(r1, a);
        debase(r2, b);
        debase(r3, mid);
    } else {
        ::size_t maxlen = std::max({a.size(), b.size(), mid.size()});
        std::string left{a};
        std::string right{b};
        std::string middle{mid};
        // fill out the strings to the maximum length
        left.resize(maxlen, ALPHABET.front());
        right.resize(maxlen, ALPHABET.back());
        middle.resize(maxlen, ALPHABET.front());
        debase(r1, left);
        debase(r2, right);
        debase(r3, middle);
    }
    // compute (middle - left) / (right - left)
    mpz_sub(r2, r2, r1);  // compute denominator
    mpz_sub(r1, r3, r1);  // compute numerator
    mpq_t result;
    mpq_init(result);
    mpz_set(mpq_numref(result), r1);  // set numerator and denominator
    mpz_set(mpq_denref(result), r2);
    double finalResult = mpq_get_d(result);
    mpq_clear(result);
    mpz_clears(r1, r2, r3, NULL);
    return finalResult;
}

template <typename InputIt>
constexpr ::size_t totalEdges(InputIt first, InputIt last) {
    return std::accumulate(first, last, static_cast<size_t>(0), [&](::size_t lhs, auto const& rhs) { return lhs + rhs.amount; });
}

template <typename T>
::size_t findLeftResult(bool const leftChosen, Edge<T> const& item, typename std::vector<Edge<T>>::const_iterator const iter, bool const leftAtBeginning, bool const leftValid, ::size_t lastLeftResult) {
    ::size_t result = 0;
    if (leftChosen) {
        // If item is from left list, set result to its value (minus the last amount removed)
        result = item.amount - lastLeftResult;
    } else {
        // Item is from right list, so its value will be somewhere between current
        // and last left amounts
        if (leftValid && !leftAtBeginning) {
            // We are positioned somewhere in the middle of the left list
            // If the label matches either, result is just 0
            if (item.label == std::prev(iter)->label || item.label == iter->label) {
                // NO - OP : compiler to optimise this branch away
            } else {
                // We have to insert this item into the left as an edge boundary
                double proportion = findStrProp(std::prev(iter)->label, iter->label, item.label);
                ::size_t amount = iter->amount;
                // amount between previous edge and "item"
                ::size_t propAmount = static_cast<::size_t>(std::lround(proportion * static_cast<double>(amount)));
                result = propAmount - lastLeftResult;
            }
        } else {
            // Left list is either at beginning or end
            // NO - OP : compiler to remove
        }
    }
    return result;
}

template <typename T>
std::vector<Edge<T>> mergeCombine(std::vector<Edge<T>> const& left, std::vector<Edge<T>> const& right) {
    static constexpr std::less comparator{};
    // sanity check
    if (!left.empty() && left.front().amount != 0) {
        throw std::invalid_argument("first left edge not 0");
    }
    if (!right.empty() && right.front().amount != 0) {
        throw std::invalid_argument("first right edge not 0");
    }
    std::vector<Edge<T>> result;
    result.reserve(left.size() + right.size());
    ::size_t prevLeftResult = 0;
    ::size_t prevRightResult = 0;
    // Walk over both lists, picking lowest item each item
    for (auto leftIt = left.cbegin(), rightIt = right.cbegin(); leftIt != left.cend() || rightIt != right.cend();) {
        bool leftValid = leftIt != left.cend();
        bool rightValid = rightIt != right.cend();
        bool chooseLeft = leftValid;
        // Select left or right list
        if (leftValid && rightValid) {
            // Both indexes valid
            chooseLeft = comparator(leftIt->label, rightIt->label);
        } else {
            // One list valid, pick one
            // chooseLeft=leftValid; //set above
        }
        // Pick correct item
        Edge<T> const& item = (chooseLeft) ? *leftIt : *rightIt;
        if (chooseLeft) {
            ++leftIt;
        } else {
            ++rightIt;
        }
        // insert into "left" slot first
        ::size_t leftAmount = findLeftResult(chooseLeft, item, leftIt, leftIt == left.cbegin(), leftValid, prevLeftResult);
        // Now insert to "right" slot
        ::size_t rightAmount = findLeftResult(!chooseLeft, item, rightIt, rightIt == right.cbegin(), rightValid, prevRightResult);
        prevLeftResult += leftAmount;
        prevRightResult += rightAmount;
        ::size_t finalAmount = leftAmount + rightAmount;
        if (result.size() > 0 && result.back().label == item.label) {
            result.back().amount += finalAmount;
        } else {
            result.push_back({item.label, finalAmount});
        }
        // Reset decrement of whichever just got bumped
        if (chooseLeft) {
            prevLeftResult = 0;
        } else {
            prevRightResult = 0;
        }
    }
    return result;
}

template <typename T>
std::vector<EdgePair<T>> makePartitions(std::vector<Edge<T>> const& histogram, ::size_t const delimit) {
    if (histogram.size() < 2) {
        throw std::invalid_argument("histogram must contain at least 2 entries");
    }
    std::vector<EdgePair<T>> result;
    auto it = ++histogram.cbegin();
    ::size_t totalEdgeWeights = totalEdges(histogram.cbegin(), histogram.cend());
    ::size_t bound_total = 0;
    T last_edge = histogram.front().label;

    for (::size_t count = delimit; count < totalEdgeWeights; count += delimit) {
        while (bound_total + it->amount < count) {
            bound_total += it->amount;
            ++it;
        }
        // Find the proportion of where this count point is between two bounds
        double prop = static_cast<double>(count - bound_total) / static_cast<double>(it->amount);
        // Calculate the mid point label
        T new_edge = midStr(std::prev(it)->label, it->label, prop);
        result.emplace_back(last_edge, new_edge);
        last_edge = new_edge;
    }
    result.emplace_back(last_edge, histogram.back().label);
    return result;
}
