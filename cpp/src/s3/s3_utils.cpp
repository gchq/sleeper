#include "s3/s3_utils.hpp"

#include <fmt/core.h>

#include <exception>

namespace gpu_compact::s3
{

Aws::SDKOptions const &getOptions() noexcept {
    static Aws::SDKOptions options = []() noexcept {
        Aws::SDKOptions v;
        v.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
        return v;
    }();
    return options;
}

void initialiseAWS() {
    Aws::InitAPI(getOptions());
}

void shutdownAWS() noexcept {
    Aws::ShutdownAPI(getOptions());
}

std::shared_ptr<Aws::S3::S3Client> makeClient() {
    return Aws::MakeShared<Aws::S3::S3Client>("");
}

Aws::String get_part(std::string_view s, std::smatch::size_type const group) {
    if (std::match_results<std::string_view::const_iterator> mr; std::regex_match(s.cbegin(), s.cend(), mr, S3_CHECK)) {
        if (mr.size() < 4) {
            throw std::logic_error("S3 URL regex failed to find 4 match groups!");
        } else {
            return mr.str(group);
        }
    } else {
        throw std::runtime_error(fmt::format("{} is not a valid S3 URL", s));
    }
}

}// namespace gpu_compact::s3