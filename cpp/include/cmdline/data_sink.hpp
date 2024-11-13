#pragma once

#include <aws/s3/S3Client.h>
#include <cudf/io/data_sink.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>

#include <concepts>
#include <memory>
#include <string>

struct SinkInfoDetails
{
    cudf::io::sink_info info;
    std::unique_ptr<cudf::io::data_sink> data_sink;
    std::unique_ptr<cudf::io::parquet_chunked_writer> writer;
};

[[nodiscard]] std::unique_ptr<cudf::io::data_sink> make_data_sink(std::convertible_to<std::string> auto const &path,
  std::shared_ptr<Aws::S3::S3Client> &s3client);

cudf::io::chunked_parquet_writer_options_builder write_opts(cudf::io::sink_info const &sink,
  cudf::io::table_metadata const &metadata) noexcept;

[[no_discard]] SinkInfoDetails make_writer(std::string const &path,
  cudf::io::table_metadata const &metadata,
  std::shared_ptr<Aws::S3::S3Client> &s3client);

[[no_discard]] SinkInfoDetails make_writer(std::convertible_to<std::string> auto const &path,
  cudf::io::table_metadata const &metadata,
  std::shared_ptr<Aws::S3::S3Client> &s3client) {
    return make_writer(path, metadata, s3client);
}
