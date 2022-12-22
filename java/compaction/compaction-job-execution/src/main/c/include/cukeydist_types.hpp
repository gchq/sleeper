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
#include <parquet/statistics.h>

#include <cstddef>
#include <cstdint>
#include <cudf/io/parquet.hpp>
#include <cudf/types.hpp>
#include <locale>
#include <map>
#include <string>
#include <utility>
#include <tuple>
#include <variant>
#include <vector>

#include "histo.hpp"
#include "type_map.hpp"

struct CommandLineInput {
    std::vector<std::string> inputFiles;
    std::vector<std::string> outputFiles;
    std::string codec = "SNAPPY";
    std::uint32_t rowGroupBytes = cudf::io::default_row_group_size_bytes;
    std::uint32_t rowGroupRows = cudf::io::default_row_group_size_rows;
    std::uint32_t pageBytes = cudf::io::default_max_page_size_bytes;
    std::vector<cudf::size_type> sortIndexes = {0};
    std::vector<std::string> splitPoints;

    template <typename T>
    void pack(T& pack) {
        // this will break if we ever try to use this to serialise!
        // zero out things otherwise the values are added
        rowGroupRows = 0;
        rowGroupBytes = 0;
        pageBytes = 0;
        inputFiles.clear();
        outputFiles.clear();
        sortIndexes.clear();
        splitPoints.clear();
        pack(inputFiles, outputFiles, codec, rowGroupBytes, rowGroupRows, pageBytes, sortIndexes, splitPoints);
    }
};

struct CommandLineOutput {
    std::vector<std::string> minKeys;
    std::vector<std::string> maxKeys;
    std::uint64_t rowsRead = 0;
    std::vector<std::uint64_t> rowsWritten;

    template <typename T>
    void pack(T& pack) {
        pack(minKeys, maxKeys, rowsRead, rowsWritten);
    }
};

struct fileinfo_t {
    std::string filepath;
    ::size_t stIndex;
    cudf::size_type rgidx;
};

using tab_mem_t = std::pair<std::unique_ptr<cudf::table>, ::size_t>;
template <typename T>
using row_mem_t = std::tuple<::size_t, ::size_t, T, T>;

/** Compile time map converts from cuDF column type to a Parquet statistics type.*/
using parquet_type_map = TypeMap<std::string,
                                 ::int32_t,
                                 ::int64_t>::To<parquet::ByteArrayStatistics,
                                                parquet::Int32Statistics,
                                                parquet::Int64Statistics>;

template <typename... ColumnTypes>
struct SortColumn {
    /**Define a variant that can take a vector of different edge types.*/
    using Edge_vec_variant = std::variant<Edge_vec<ColumnTypes>...>;
    /**Define a variant of vector of vector edge pairs.*/
    using EdgePair_vec2_variant = std::variant<std::vector<EdgePair_vec<ColumnTypes>>...>;
};

using SupportedTypes = SortColumn<std::string, ::int32_t, ::int64_t>;

enum class MergeResult { Success,
                         AllocFail };

class comma_numpunct : public std::numpunct<char> {
   protected:
    virtual char do_thousands_sep() const override {
        return ',';
    }

    virtual std::string do_grouping() const override {
        return "\03";
    }
};
