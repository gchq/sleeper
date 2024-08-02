#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace gpu_compact::cudf_compact {
using std::vector;
using std::string;

using PartitionBound = std::variant<int32_t, int64_t, string, vector<uint8_t>, std::monostate>;

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
    vector<string> input_files;
    string output_file;
    vector<string> row_key_cols;
    vector<string> sort_key_cols;
    std::size_t max_row_group_size;
    std::size_t max_page_size;
    string compression;
    string writer_version;
    std::size_t column_truncate_length;
    std::size_t stats_truncate_length;
    bool dict_enc_row_keys;
    bool dict_enc_sort_keys;
    bool dict_enc_values;
    std::unordered_map<string, ColRange> region;
};// struct CompactionInput

}// namespace gpu_compact::cudf_compact