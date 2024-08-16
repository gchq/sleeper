#pragma once

#include <cudf/io/types.hpp>
#include <fmt/core.h>

#include "cudf_compact/s3/s3_sink.hpp"

#include <concepts>
#include <exception>
#include <regex>
#include <string>

namespace gpu_compact::cudf_compact {

inline const std::regex URL_CHECK(R"(([a-z][a-z0-9+\-.]*)://(/?(-.)?([^\s/?.#-]+.?)+(/[^\s]*)?))");

[[nodiscard]] inline cudf::io::sink_info make_sink_info(std::convertible_to<std::string> auto const &path) {
    // test for which protocol
    if (path.starts_with("file://")) {
        return cudf::io::sink_info{ path.substr(7) };
    } else if (path.starts_with("s3://")) {
        s3::S3Sink a{ path };
        return cudf::io::sink_info{ path };
    } else {
        throw std::runtime_error(fmt::format("Unknown URL schema {}", path));
    }
}

}// namespace gpu_compact::cudf_compact
