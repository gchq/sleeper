#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <cudf/io/data_sink.hpp>
#include <cudf/io/types.hpp>
#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include "io/s3_sink.hpp"

#include <concepts>
#include <exception>
#include <memory>
#include <regex>
#include <string>

namespace gpu_compact::cudf_compact
{

inline const std::regex URL_CHECK(R"(([a-z][a-z0-9+\-.]*)://(/?(-.)?([^\s/?.#-]+.?)+(/[^\s]*)?))");

struct SinkInfoDetails
{
    cudf::io::sink_info info;
    std::unique_ptr<cudf::io::data_sink> sink;
};

[[nodiscard]] inline SinkInfoDetails make_sink_info(std::convertible_to<std::string> auto const &path,
  std::shared_ptr<Aws::S3::S3Client> &s3client) {
    // test for which protocol
    std::unique_ptr<cudf::io::data_sink> sink;
    if (path.starts_with("file://")) {
        sink = cudf::io::data_sink::create(path.substr(7));
    } else if (path.starts_with("s3://")) {
        sink = std::make_unique<s3::S3Sink>(s3client, path);
    } else {
        throw std::runtime_error(fmt::format("Unknown URL schema {}", path));
    }
    return { cudf::io::sink_info(sink.get()), std::move(sink) };
};

}// namespace gpu_compact::cudf_compact
