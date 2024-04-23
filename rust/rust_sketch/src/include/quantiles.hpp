/*
 * Copyright 2022-2024 Crown Copyright
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
 * @file quantiles.hpp
 * @brief Contains the wrapping and helper functions to facilitate mapping from Rust to C++
 * for quantiles sketches.
 * @date 2024
 *
 * @copyright Copyright 2022-2024 Crown Copyright
 *
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "byte_array_serializer.hpp"
#include "common_types.hpp"
#include "quantiles_sketch.hpp"
#include "rust/cxx.h"

namespace rust_sketch {

/**
 * @brief Use our custom byte_array serializer for byte_array type, otherwise fallback to default one provided by datasketches library.
 * @tparam T element type of sketch
 */
template <typename T>
using serializer_t = std::conditional_t<std::is_same_v<T, byte_array>, ByteArraySerde, datasketches::serde<T>>;

/**
 * @brief Datasketch derived class to provide Rust support for serialization.
 *
 * @tparam T the element type of the quantiles sketch
 * @tparam C the comparator for type T
 * @tparam A the allocator for memory
 */
template <typename T, typename C, typename A>
struct serialization_bridge : public datasketches::quantiles_sketch<T, C, A> {
    /**
     * @brief Construct a new serialization bridge object.
     *
     * @param k the size of the datasketch
     * @param comparator item comparator
     * @param allocator memory allocator
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     */
    serialization_bridge(uint16_t k, const C& comparator, const A& allocator) : datasketches::quantiles_sketch<T, C, A>(k, comparator, allocator) {}

    /**
     * @brief Convert a base class sketch into an instance of this class.
     *
     * This is provided as a convenience function for deserialization.
     *
     * @param rv rvalue of base class
     */
    explicit serialization_bridge(datasketches::quantiles_sketch<T, C, A>&& rv) : datasketches::quantiles_sketch<T, C, A>(std::move(rv)) {}

    /**
     * @brief Get the size of the serialized sketch.
     *
     * @return number of bytes
     */
    ::size_t get_serialized_size_bytes() const {
        return datasketches::quantiles_sketch<T, C, A>::get_serialized_size_bytes();
    }

    /**
     * @brief Serialize the sketch to a Rust compatible byte array.
     *
     * This created a Rust vector to act as a byte array and then serializes the sketch to it.
     *
     * @param header_size_bytes number of padding bytes to add to serialized array
     * @return rust byte array
     * @exception std::runtime_error if serialization could not succeed
     */
    rust::Vec<::uint8_t> serialize(std::uint32_t header_size_bytes) const {
        auto bytes = datasketches::quantiles_sketch<T, C, A>::template serialize<serializer_t<T>>(header_size_bytes);
        rust::Vec<::uint8_t> output{};
        std::copy(bytes.cbegin(), bytes.cend(), std::back_inserter(output));
        return output;
    }
};

/**
 * @brief Convenience type defined for the `serialization_bridge` derived class.
 *
 * @tparam T the element type of the quantiles sketch
 * @tparam C the comparator for type T
 * @tparam A the allocator for memory
 */
template <typename T, typename C, typename A>
using base_type = serialization_bridge<T, C, A>;

/**
 * @brief Derived sketch class intended for fixed width integer sketches.
 *
 * This provides some wrapping functions to allow for convenient mapping between
 * Rust types and C++. Rust can't pass certain values by reference, so we take
 * them by value here and then call down to the C++ layer by reference.
 *
 * @tparam T the element type of the quantiles sketch
 * @tparam C the comparator for type T
 * @tparam A the allocator for memory
 */
template <typename T, typename C = std::less<T>, typename A = std::allocator<T>>
struct quantiles_sketch_derived : public base_type<T, C, A> {
    /**
     * @brief Allows access to non-template base class type
     */
    using pop_base = base_type<T, C, A>;

    /**
     * @brief Construct a new quantiles sketch derived object.
     *
     * @param k the size of the datasketch
     * @param comparator item comparator
     * @param allocator memory allocator
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     */
    quantiles_sketch_derived(uint16_t k = datasketches::quantiles_constants::DEFAULT_K,
                             const C& comparator = C(), const A& allocator = A()) : pop_base(k, comparator, allocator) {}

    /**
     * @brief Convert a base class sketch into an instance of this class.
     *
     * This is provided as a convenience function for deserialization.
     *
     * @param rv rvalue of base class
     */
    explicit quantiles_sketch_derived(datasketches::quantiles_sketch<T, C, A>&& rv) : pop_base(std::move(rv)) {}

    /**
     * @brief Allows accepting types by value.
     *
     * The underlying update method takes values by forwarding reference (&&)
     * which has no Rust equivalent so we have to wrap the function to allow
     * binding from Rust.
     *
     * @tparam FwdT fixed width integer type
     * @param item the item to add to the sketch
     */
    template <typename FwdT>
    void update(FwdT item) {
        pop_base::update(item);
    }

    /**
     * @brief Get the minimum item in the sketch.
     *
     * The underlying method returns a reference, so we have to convert it to a value.
     *
     * @return T minimum item
     * @exception std::runtime_error if sketch is empty
     */
    T get_min_item() const {
        return pop_base::get_min_item();
    }

    /**
     * @brief Get the maximum item in the sketch.
     *
     * The underlying method returns a reference, so we have to convert it to a value.
     *
     * @return T maximum item
     * @exception std::runtime_error if sketch is empty
     */
    T get_max_item() const {
        return pop_base::get_max_item();
    }

    /**
     * @brief Get the rank of a given item.
     *
     * Converts the item to reference form needed in underlying function.
     *
     * @param item item to get rank of
     * @param inclusive see link
     * @return double rank of item
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     * @exception std::runtime_error if sketch is empty
     */
    double get_rank(T const item, bool inclusive = true) const {
        return pop_base::get_rank(item, inclusive);
    }

    /**
     * @brief Merges another sketch into this one.
     *
     * @param other the other sketch to merge into this one
     */
    template <typename FwdSk>
    void merge(FwdSk&& other) {
        pop_base::merge(std::forward<FwdSk>(other));
    }
};

/**
 * @brief Derived sketch class intended for std::string sketches.
 *
 * This is a partial specialization of the class template `quantiles_sketch_derived`.
 *
 * This provides some wrapping functions to allow for convenient mapping between
 * Rust types and C++. Rust can't pass certain values by reference, so we take
 * them by value here and then call down to the C++ layer by reference.
 *
 * @tparam C the comparator for type std::string
 * @tparam A the allocator for memory
 */
template <typename C, typename A>
struct quantiles_sketch_derived<std::string, C, A> : public base_type<std::string, C, A> {
    /**
     * @brief Allows access to non-template base class type
     */
    using pop_base = base_type<std::string, C, A>;

    /**
     * @brief Construct a new quantiles sketch derived object.
     *
     * @param k the size of the datasketch
     * @param comparator item comparator
     * @param allocator memory allocator
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     */
    quantiles_sketch_derived(uint16_t k = datasketches::quantiles_constants::DEFAULT_K,
                             const C& comparator = C(), const A& allocator = A()) : pop_base(k, comparator, allocator) {}

    /**
     * @brief Convert a base class sketch into an instance of this class.
     *
     * This is provided as a convenience function for deserialization.
     *
     * @param rv rvalue of base class
     */
    explicit quantiles_sketch_derived(datasketches::quantiles_sketch<std::string, C, A>&& rv) : pop_base(std::move(rv)) {}

    /**
     * @brief Get the minimum item in the sketch.
     *
     * The underlying method returns a reference, so we have to convert it to a value.
     *
     * @return minimum item
     * @exception std::runtime_error if sketch is empty
     */
    rust::String get_min_item() const {
        return pop_base::get_min_item();
    }

    /**
     * @brief Get the maximum item in the sketch.
     *
     * The underlying method returns a reference, so we have to convert it to a value.
     *
     * @return maximum item
     * @exception std::runtime_error if sketch is empty
     */
    rust::String get_max_item() const {
        return pop_base::get_max_item();
    }

    /**
     * @brief Get the rank of a given item.
     *
     * Converts the item to reference form needed in underlying function.
     *
     * @param item item to get rank of
     * @param inclusive see link
     * @return double rank of item
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     * @exception std::runtime_error if sketch is empty
     */
    double get_rank(rust::Str const item, bool inclusive = true) const {
        return pop_base::get_rank(std::string(item), inclusive);
    }

    /**
     * @brief The item that would be approximately at the given quantile.
     *
     * @param rank quantile rank to retrieve
     * @param inclusive
     * @return string that would be at the given quantile
     * @exception std::runtime_error if the sketch is empty
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     */
    rust::String get_quantile(double rank, bool inclusive = true) const {
        // implicit conversion from std::string & to rust::String
        return pop_base::get_quantile(rank, inclusive);
    }

    /**
     * @brief Allows accepting types by value.
     *
     * The underlying update method takes values by forwarding reference (&&)
     * which has no Rust equivalent so we have to wrap the function to allow
     * binding from Rust.
     *
     * @param item the item to add to the sketch
     */
    void update(rust::Str const item) {
        pop_base::update(std::string(item));
    }
};

/**
 * @brief Derived sketch class intended for std::vector<uint8_t> sketches.
 *
 * This is a partial specialization of the class template `quantiles_sketch_derived`.
 *
 * This provides some wrapping functions to allow for convenient mapping between
 * Rust types and C++. Rust can't pass certain values by reference, so we take
 * them by value here and then call down to the C++ layer by reference.
 *
 * @tparam C the comparator for type std::string
 * @tparam A the allocator for memory
 */
template <typename C, typename A>
struct quantiles_sketch_derived<byte_array, C, A> : public base_type<byte_array, C, A> {
    /**
     * @brief Allows access to non-template base class type
     */
    using pop_base = base_type<byte_array, C, A>;

    /**
     * @brief Construct a new quantiles sketch derived object.
     *
     * @param k the size of the datasketch
     * @param comparator item comparator
     * @param allocator memory allocator
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     */
    quantiles_sketch_derived(uint16_t k = datasketches::quantiles_constants::DEFAULT_K,
                             const C& comparator = C(), const A& allocator = A()) : pop_base(k, comparator, allocator) {}

    /**
     * @brief Convert a base class sketch into an instance of this class.
     *
     * This is provided as a convenience function for deserialization.
     *
     * @param rv rvalue of base class
     */
    explicit quantiles_sketch_derived(datasketches::quantiles_sketch<byte_array, C, A>&& rv) : pop_base(std::move(rv)) {}

    /**
     * @brief Get the minimum item in the sketch.
     *
     * The underlying method returns a reference, so we have to convert it to a value.
     *
     * @return minimum item
     * @exception std::runtime_error if sketch is empty
     */
    rust::Vec<uint8_t> get_min_item() const {
        return toRustVec(pop_base::get_min_item());
    }

    /**
     * @brief Get the maximum item in the sketch.
     *
     * The underlying method returns a reference, so we have to convert it to a value.
     *
     * @return maximum item
     * @exception std::runtime_error if sketch is empty
     */
    rust::Vec<::uint8_t> get_max_item() const {
        return toRustVec(pop_base::get_max_item());
    }

    /**
     * @brief Get the rank of a given item.
     *
     * Converts the item to reference form needed in underlying function.
     *
     * @param item item to get rank of
     * @param inclusive see link
     * @return double rank of item
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     * @exception std::runtime_error if sketch is empty
     */
    double get_rank(rust::Slice<::uint8_t const> const item, bool inclusive = true) const {
        return pop_base::get_rank(byte_array{item.begin(), item.end()}, inclusive);
    }

    /**
     * @brief The item that would be approximately at the given quantile.
     *
     * @param rank quantile rank to retrieve
     * @param inclusive
     * @return byte array that would be at the given quantile
     * @exception std::runtime_error if the sketch is empty
     * @see https://github.com/apache/datasketches-cpp/blob/master/quantiles/include/quantiles_sketch.hpp
     */
    rust::Vec<::uint8_t> get_quantile(double rank, bool inclusive = true) const {
        return toRustVec(pop_base::get_quantile(rank, inclusive));
    }

    /**
     * @brief Allows accepting types by value.
     *
     * The underlying update method takes values by forwarding reference (&&)
     * which has no Rust equivalent so we have to wrap the function to allow
     * binding from Rust.
     *
     * @param item the item to add to the sketch
     */
    void update(rust::Slice<::uint8_t const> const item) {
        pop_base::update(byte_array{item.begin(), item.end()});
    }

   private:
    /**
     * @brief Helper function to convert a C++ byte array to a Rust Vector.
     *
     * @param vec C++ vector
     * @return Rust byte array
     */
    static rust::Vec<::uint8_t> toRustVec(byte_array const& vec) {
        rust::Vec<::uint8_t> output{};
        std::copy(vec.cbegin(), vec.cend(), std::back_inserter(output));
        return output;
    }
};

/**
 * @brief Type for datasketch based on 32 bit integers
 */
using i32_sketch_t = quantiles_sketch_derived<::int32_t>;
/**
 * @brief Type for datasketch based on 64 bit integers
 */
using i64_sketch_t = quantiles_sketch_derived<::int64_t>;
/**
 * @brief Type for datasketch based on strings
 */
using string_sketch_t = quantiles_sketch_derived<std::string>;
/**
 * @brief Type for datasketch based on byte arrays
 */
using byte_sketch_t = quantiles_sketch_derived<byte_array>;

/**
 * @brief Create a new datasketch of a particular type wrapped in a smart pointer.
 *
 * @tparam T sketch type (NOT it's element type) e.g. i32_sketch_t
 * @param K size of sketch to create
 * @return RAII compatible smart pointer for sketch
 */
template <typename T>
constexpr std::unique_ptr<T> new_quantiles_sketch(::uint16_t const K) noexcept {
    return std::make_unique<T>(K);
}

/**
 * @brief Deserialize a data sketch.
 *
 * @tparam T sketch type (NOT it's element type) e.g. i32_sketch_t
 * @param bytes Rust byte array to deserialize from
 * @return a created datasketch object
 * @exception std::runtime_error if deserialization fails
 */
template <typename T>
std::unique_ptr<T> deserialize(rust::Slice<::uint8_t const> const bytes) {
    // parameter T is one of the derived sketch classes, e.g. i32_sketch_t
    // deserialize is defined in the base class in datasketches namespace
    return std::make_unique<T>(T::template deserialize<serializer_t<typename T::value_type>>(bytes.data(), bytes.size()));
}

} /* namespace rust_sketch */
