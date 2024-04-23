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
 * @file byte_array_serializer.hpp
 * @brief A serializer for byte array sketches. This was adapted from datasketches::serde<std::string>
 * @date 2023
 *
 * @copyright Copyright 2022-2024 Crown Copyright
 *
 */
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>

#include "common_types.hpp"
#include "memory_operations.hpp"

namespace rust_sketch {

/**
 * @brief Serializer implementation for byte arrays.
 *
 * Adapted from datasketches::serde<std::string>
 */
struct ByteArraySerde {
    /**
     * @brief Serialize a contiguously stored set of byte arrays to the output stream.
     *
     * Serializes the byte_arrays in the array to the output stream. The serialized format
     * consists of a 4 byte length field (platform endianness) followed by the raw byte
     * contents. As byte_array is itself a `std::vector<uint8_t>` with data stored on
     * the heap, we read the data with `items[i].data()`.
     *
     * If an error occurs during this function, all created objects in the `items`
     * in the buffer will be destroyed.
     *
     * @param os stream to serialize to
     * @param items pointer to array of start of data items
     * @param num number of items in array
     * @exception std::runtime_error is something goes wrong during serialization
     */
    void serialize(std::ostream& os, const byte_array* items, unsigned const num) const {
        for (::size_t i = 0; i < num && os.good(); i++) {
            try {
                uint32_t length = static_cast<uint32_t>(items[i].size());
                os.write(reinterpret_cast<char const*>(&length), sizeof(length));
                os.write(reinterpret_cast<char const*>(items[i].data()), length);
            } catch (std::ostream::failure const& e) {
                throw std::runtime_error("error writing item " + std::to_string(i) + ": " + e.what());
            }
            // abort early if there is any indication of failure
            if (!os.good()) {
                throw std::runtime_error("error writing to std::ostream at item " + std::to_string(i));
            }
        }
    }

    /**
     * @brief Deserializes a quantiles sketch from the input stream.
     *
     * The `items` pointer **MUST** have allocated enough space for `num`
     * instances of `std::vector<uint8_t>` although they shouldn't be created.
     * This function will use `std::unitialized_value_construct` to construct
     * objects in the buffer. The data from the stream is then read into each
     * vector to create the data sketch.
     *
     * If an error occurs during this function, all created objects in the `items`
     * in the buffer will be destroyed.
     *
     * @param is the input stream to read from
     * @param items the allocated but not initialized data buffer
     * @param num the number of items to deserialize
     * @exception std::runtime_error is something goes wrong during deserialization
     */
    void deserialize(std::istream& is, byte_array* items, unsigned const num) const {
        // initialise the objects in memory
        std::uninitialized_value_construct_n(items, num);
        // if something goes wrong, this contains the item count
        size_t errorIndex = 0;
        try {
            for (::size_t i = 0; i < num; i++) {
                // read length
                ::uint32_t length = 0;
                is.read(reinterpret_cast<char*>(&length), sizeof(length));
                if (!is.good()) {
                    errorIndex = i;
                    break;
                }
                // read data
                items[i].resize(length);
                is.read(reinterpret_cast<char*>(items[i].data()), length);
                if (is.gcount() != length || !is.good()) {
                    errorIndex = i;
                    break;
                }
            }
        } catch (std::ostream::failure const& e) {
            std::destroy_n(items, num);
            throw std::runtime_error("error reading item " + std::to_string(errorIndex) + ": " + e.what());
        }
        if (!is.good()) {
            std::destroy_n(items, num);
            throw std::runtime_error("error reading from std::istream at item " + std::to_string(errorIndex));
        }
    }

    /**
     * @brief Find the serialized size of the given item.
     *
     * This gives the space needed to serialize one item which is the
     * length of byte array plus 4 bytes for the length.
     *
     * @param item item to test
     * @return size_t serialized size
     */
    size_t size_of_item(const byte_array& item) const noexcept {
        return sizeof(uint32_t) + item.size();
    }

    /**
     * @brief Serialize an array of byte arrays to the given byte buffer
     *
     * Copies the byte arrays in serialized form to a given byte array. We include
     * size checks to ensure we don't overwrite the end of the buffer.
     *
     * @param ptr byte buffer to write to
     * @param capacity the length of the buffer
     * @param items array of byte arrays to serialize
     * @param num number of items in the array
     * @return size_t the number of bytes written
     * @exception std::out_of_range if we run out of space in the serialization buffer
     */
    size_t serialize(void* ptr, size_t capacity, byte_array const* items, unsigned const num) const {
        size_t bytes_written = 0;
        for (::size_t i = 0; i < num; ++i) {
            const uint32_t length = static_cast<uint32_t>(items[i].size());
            const size_t new_bytes = length + sizeof(length);
            datasketches::check_memory_size(bytes_written + new_bytes, capacity);
            memcpy(ptr, &length, sizeof(length));
            ptr = static_cast<char*>(ptr) + sizeof(uint32_t);
            memcpy(ptr, items[i].data(), length);
            ptr = static_cast<char*>(ptr) + length;
            bytes_written += new_bytes;
        }
        return bytes_written;
    }

    /**
     * @brief Deserializes byte arrays from the given byte buffer
     *
     * This copies the byte data from the buffer into std::vectors stored
     * in the `items` array.
     *
     * This array should be allocated but **NOT** initialized before calling this method.
     * Any created objects will be destroyed when an exception is thrown.
     *
     * @param ptr byte buffer containing serialized data
     * @param capacity length of byte buffer
     * @param items the array of items to deserialize to
     * @param num number of items to deserialize from the buffer
     * @return size_t number of bytes read
     * @exception std::out_of_range if we run out of buffer to read
     */
    size_t deserialize(const void* ptr, size_t capacity, byte_array* items, unsigned const num) const {
        size_t bytes_read = 0;
        // initialise the objects in memory
        std::uninitialized_value_construct_n(items, num);
        try {
            for (::size_t i = 0; i < num; i++) {
                // read length
                ::uint32_t length = 0;
                // do we have enough bytes (4) available in buffer capacity?
                datasketches::check_memory_size(bytes_read + sizeof(length), capacity);
                memcpy(&length, ptr, sizeof(length));
                bytes_read += sizeof(length);
                // move pointer
                ptr = static_cast<const char*>(ptr) + sizeof(length);
                // do we have enough bytes for byte array in buffer capacity?
                datasketches::check_memory_size(bytes_read + length, capacity);
                // vector was initialised at top of function
                // so now, resize it and memcmp the data straight in
                items[i].resize(length);
                memcpy(items[i].data(), ptr, length);
                // move pointer
                ptr = static_cast<const char*>(ptr) + length;
                bytes_read += length;
            }
        } catch (...) {
            // catch the exception and destroy the items before rethrowing
            std::destroy_n(items, num);
            throw;
        }

        return bytes_read;
    }
};
} /* namespace rust_sketch */
