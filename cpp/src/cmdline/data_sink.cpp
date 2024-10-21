#include "data_sink.hpp"

#include "s3/s3_sink.hpp"

#include <utility>

[[nodiscard]] std::unique_ptr<cudf::io::data_sink> make_data_sink(std::convertible_to<std::string> auto const &path,
  std::shared_ptr<Aws::S3::S3Client> &s3client) {
    if (path.starts_with("s3://")) {
        return std::make_unique<gpu_compact::s3::S3Sink>(s3client, path);
    } else {
        return cudf::io::data_sink::create(path);
    }
}

cudf::io::chunked_parquet_writer_options_builder write_opts(cudf::io::sink_info const &sink) noexcept {
    return cudf::io::chunked_parquet_writer_options::builder(sink)
      .compression(cudf::io::compression_type::ZSTD)
      .row_group_size_bytes(65 * 1'048'576)
      .row_group_size_rows(1'000'000)
      .max_page_size_bytes(512 * 1024)
      .max_page_size_rows(20'000)
      .stats_level(cudf::io::statistics_freq::STATISTICS_COLUMN)
      .write_v2_headers(true)
      .dictionary_policy(cudf::io::dictionary_policy::ADAPTIVE);
}

[[no_discard]] SinkInfoDetails make_writer(std::string const &path, std::shared_ptr<Aws::S3::S3Client> &s3client) {
    auto &&data_sink = make_data_sink(path, s3client);
    cudf::io::sink_info sink{ &*data_sink };
    auto wopts = write_opts(sink);
    return { sink,
        std::forward<decltype(data_sink)>(data_sink),
        std::make_unique<cudf::io::parquet_chunked_writer>(wopts.build()) };
}
