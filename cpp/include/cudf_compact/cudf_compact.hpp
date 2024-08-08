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
    vector<string> inputFiles;
    string outputFile;
    vector<string> rowKeyCols;
    vector<string> sortKeyCols;
    std::size_t maxRowGroupSize;
    std::size_t maxPageSize;
    string compression;
    string writerVersion;
    std::size_t columnTruncateLength;
    std::size_t statsTruncateLength;
    bool dictEncRowKeys;
    bool dictEncSortKeys;
    bool dictEncValues;
    std::unordered_map<string, ColRange> region;
};// struct CompactionInput

CompactionResult merge_sorted_files(CompactionInput const &inputData);
}// namespace gpu_compact::cudf_compact