#pragma once

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "cudf_compact/parquet_types.h"

#include <cstddef>
#include <cstdint>
#include <cudf/types.hpp>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace gpu_compact::cudf_compact
{

typedef struct
{
    std::string filepath;
    size_t fileidx;
    cudf::size_type rgidx;
} fileinfo_t;

using stats_t = std::pair<std::string, std::string>;
using statsvec_t = std::vector<stats_t>;
using filestats_t = std::vector<statsvec_t>;

constexpr uint32_t parquet_magic = (('P' << 0) | ('A' << 8) | ('R' << 16) | ('1' << 24));

struct file_header_s
{
    uint32_t magic;
};

struct file_ender_s
{
    uint32_t footer_len;
    uint32_t magic;
};

class SeekableMemoryBuffer : public apache::thrift::transport::TMemoryBuffer
{
  private:
    std::size_t _length;

  public:
    SeekableMemoryBuffer(std::size_t len) : TMemoryBuffer(static_cast<uint32_t>(len)), _length(len) {}

    inline void seek(uint32_t offset) {
        resetBuffer();// sets read/write ptrs to 0
        wroteBytes(static_cast<uint32_t>(_length));// sets write ptr to length (bytes now available)
        resetConsumedMessageSize(-1);// tell transport we're clean again
        if (offset == 0) {
            return;
        }
        uint8_t *foo{ nullptr };
        uint32_t len{ offset };
        borrow(foo, &len);
        consume(offset);// sets read ptr to offset
    }
};

inline bool read_footer(std::ifstream &source, std::string const &filepath, parquet::format::FileMetaData &fmd) {
    using apache::thrift::protocol::TCompactProtocol;
    using apache::thrift::transport::TMemoryBuffer;
    constexpr auto header_len = sizeof(file_header_s);
    constexpr auto ender_len = sizeof(file_ender_s);

    const auto len = std::filesystem::file_size(filepath);

    file_header_s header;
    source.read(reinterpret_cast<char *>(&header), header_len);
    file_ender_s ender;
    source.seekg(static_cast<std::streamoff>(len - ender_len));
    source.read(reinterpret_cast<char *>(&ender), ender_len);

    // checks for valid header, footer, and file length
    assert(len > header_len + ender_len);
    assert(header.magic == parquet_magic && ender.magic == parquet_magic);
    assert(ender.footer_len != 0 && ender.footer_len <= (len - header_len - ender_len));

    // parquet files end with 4-byte footer_length and 4-byte magic == "PAR1"
    // seek backwards from the end of the file (footer_length + 8 bytes of ender)
    std::shared_ptr<TMemoryBuffer> strBuf(new TMemoryBuffer(ender.footer_len));
    source.seekg(static_cast<std::streamoff>(len - ender_len - ender.footer_len));
    source.read(reinterpret_cast<char *>(strBuf->getWritePtr(ender.footer_len)), ender.footer_len);
    strBuf->wroteBytes(ender.footer_len);

    TCompactProtocol proto{ strBuf };
    fmd.read(&proto);
    return true;
}

inline parquet::format::FileMetaData read_footer(std::string const &filepath) {
    std::ifstream source(filepath, std::ios::binary);
    parquet::format::FileMetaData fmd;
    read_footer(source, filepath, fmd);
    return fmd;
}

inline std::tuple<parquet::format::FileMetaData,
  std::vector<parquet::format::OffsetIndex>,
  std::vector<parquet::format::ColumnIndex>>
  read_indexes(std::string const &filepath) {
    using apache::thrift::protocol::TCompactProtocol;
    std::ifstream source(filepath, std::ios::binary);
    parquet::format::FileMetaData fmd;
    read_footer(source, filepath, fmd);

    std::vector<parquet::format::OffsetIndex> offset_index;
    std::vector<parquet::format::ColumnIndex> column_index;

    int64_t const min_offset = fmd.row_groups.front().columns.front().column_index_offset;
    auto const &last_col = fmd.row_groups.back().columns.back();
    int64_t const max_offset = last_col.offset_index_offset + last_col.offset_index_length;

    size_t const length = static_cast<size_t>(max_offset - min_offset);
    auto idx_buf = std::unique_ptr<char>(new char[length]);

    std::shared_ptr<SeekableMemoryBuffer> buf(new SeekableMemoryBuffer(length));
    source.seekg(min_offset);
    source.read(
      reinterpret_cast<char *>(buf->getWritePtr(static_cast<uint32_t>(length))), static_cast<uint32_t>(length));

    TCompactProtocol proto{ buf };

    // find size metadata
    for (auto const &rg : fmd.row_groups) {
        for (auto const &col : rg.columns) {
            auto const oi_offset = col.offset_index_offset - min_offset;
            auto const oi_len = col.offset_index_length;
            if (oi_len > 0 and oi_offset >= 0) {
                parquet::format::OffsetIndex oi;
                buf->seek(static_cast<uint32_t>(oi_offset));
                oi.read(&proto);
                offset_index.push_back(std::move(oi));
            } else {
                offset_index.push_back({});
            }

            auto const ci_offset = col.column_index_offset - min_offset;
            auto const ci_len = col.column_index_length;
            if (ci_len > 0 and ci_offset >= 0) {
                parquet::format::ColumnIndex ci;
                buf->seek(static_cast<uint32_t>(ci_offset));
                ci.read(&proto);
                column_index.push_back(std::move(ci));
            } else {
                column_index.push_back({});
            }
        }
    }
    return { std::move(fmd), std::move(offset_index), std::move(column_index) };
}

// takes a flattened parquet schema as retrieved from the FileMetaData and prunes out all but
// the leaf elements.
inline std::vector<parquet::format::SchemaElement> trim_schema(
  std::vector<parquet::format::SchemaElement> const &schema) {
    std::vector<parquet::format::SchemaElement> result;
    for (auto const &elem : schema) {
        if (elem.num_children == 0) {
            result.push_back(elem);
        }
    }
    return result;
}

}// namespace gpu_compact::cudf_compact