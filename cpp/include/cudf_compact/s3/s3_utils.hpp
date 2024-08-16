#pragma once

#include <aws/core/Aws.h>

#include <concepts>
#include <regex>
#include <string_view>

namespace gpu_compact::cudf_compact::s3 {

inline const std::regex URL_CHECK(R"((s3):///?[-.]?([^\s?.#-]+?)/([^\s]+))");

Aws::String get_part(std::string_view const s, std::smatch::size_type group);

inline Aws::String getBucket(std::convertible_to<std::string_view> auto const &path) {
    return get_part(path, 2);
}

inline Aws::String getKey(std::convertible_to<std::string_view> auto const &path) {
    return get_part(path, 3);
}

}// namespace gpu_compact::cudf_compact::s3