#pragma once

#include <aws/core/Aws.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <spdlog/spdlog.h>

#include "cudf_compact/format_helper.hpp"

#include <concepts>
#include <exception>
#include <memory>
#include <regex>
#include <string_view>
#include <typeinfo>

namespace gpu_compact::cudf_compact::s3
{

inline const std::regex S3_CHECK(R"((s3):///?[-.]?([^\s?#]+?)/([^\s]+))");

void initialiseAWS();

void shutdownAWS() noexcept;

std::shared_ptr<Aws::S3::S3Client> makeClient();

Aws::String get_part(std::string_view const s, std::smatch::size_type group);

inline Aws::String getBucket(std::convertible_to<std::string_view> auto const &path) {
    return get_part(path, 2);
}

inline Aws::String getKey(std::convertible_to<std::string_view> auto const &path) {
    return get_part(path, 3);
}

template<typename T, typename E> inline T const &unwrap(Aws::Utils::Outcome<T, E> const &outcome) noexcept(false) {
    if (outcome.IsSuccess()) {
        return outcome.GetResult();
    } else {
        E error = outcome.GetError();
        SPDLOG_ERROR(ff(
          "Error unwrapping a {}, threw {}: {}", typeid(outcome).name(), error.GetExceptionName(), error.GetMessage()));
        throw std::runtime_error(ff("{}: {}", error.GetExceptionName(), error.GetMessage()));
    }
}

}// namespace gpu_compact::cudf_compact::s3