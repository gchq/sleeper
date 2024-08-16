#include "cudf_compact/s3/s3_utils.hpp"

#include <fmt/core.h>

#include <exception>

namespace gpu_compact::cudf_compact::s3 {

Aws::String get_part(std::string_view s, std::smatch::size_type const group) {
    if (std::match_results<std::string_view::const_iterator> mr;
        std::regex_match(s.cbegin(), s.cend(), mr, URL_CHECK)) {
        if (mr.size() < 4) {
            throw std::logic_error("S3 URL regex failed to find 4 match groups!");
        } else {
            return mr.str(group);
        }
    } else {
        throw std::runtime_error(fmt::format("{} is not a valid S3 URL", s));
    }
}


}// namespace gpu_compact::cudf_compact::s3