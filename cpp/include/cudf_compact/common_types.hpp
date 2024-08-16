#pragma once

#include <cudf/scalar/scalar.hpp>

#include "cudf_compact/parquet_types.h"

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <variant>
#include <vector>

namespace gpu_compact::cudf_compact {

using scalar_ptr = std::shared_ptr<cudf::scalar>;
using scalar_pair = std::tuple<std::string, scalar_ptr, std::string, scalar_ptr>;

using PartitionBound = std::variant<int32_t, int64_t, std::string, std::vector<uint8_t>, std::monostate>;

struct ColRange
{
  public:
    PartitionBound lower;
    bool lower_inclusive;
    PartitionBound upper;
    bool upper_inclusive;
};// struct ColRange

struct CompactionResult
{
  public:
    std::size_t rows_read;
    std::size_t rows_written;
};// struct CompactionResult

struct CompactionInput
{
  public:
    std::vector<std::string> inputFiles;
    std::string outputFile;
    std::vector<std::string> rowKeyCols;
    std::vector<std::string> sortKeyCols;
    std::size_t maxRowGroupSize;
    std::size_t maxPageSize;
    std::string compression;
    std::string writerVersion;
    std::size_t columnTruncateLength;
    std::size_t statsTruncateLength;
    bool dictEncRowKeys;
    bool dictEncSortKeys;
    bool dictEncValues;
    std::unordered_map<std::string, ColRange> region;
};// struct CompactionInput

struct col_schema
{
    parquet::format::SchemaElement se;
    int max_def;
    int max_rep;
};

struct page_info
{
    int file_idx;// file this page comes from
    int rg_idx;// row group this page comes from
    int col_idx;// column index within file this page comes from
    int page_idx;// index of page within the column
    int schema_idx;// schema index for this column
    int global_col_idx;// column index wrt all files (key)
    int row_count;
    size_t size_bytes;
};

inline std::ostream &operator<<(std::ostream &out, page_info const &p)
{
    return out << "file_idx " << p.file_idx << " rg_idx " << p.rg_idx << " col_idx " << p.col_idx << " page_idx "
               << p.page_idx << " schema_idx " << p.schema_idx << " global_col_idx " << p.global_col_idx
               << " row_count " << p.row_count << " size_bytes " << p.size_bytes;
}

}// namespace gpu_compact::cudf_compact