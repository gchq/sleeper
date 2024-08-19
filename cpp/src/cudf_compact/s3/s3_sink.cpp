#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <spdlog/spdlog.h>

#include "cudf_compact/s3/s3_sink.hpp"
#include "cudf_compact/s3/s3_utils.hpp"


#include <exception>

namespace gpu_compact::cudf_compact::s3
{

// struct S3Sink final : public cudf::io::data_sink
// {
//   private:
//     std::shared_ptr<Aws::S3::S3Client> client;
// Aws::String bucket;
// Aws::String key;
// Aws::String uploadId;
// Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;
// Aws::Vector<std::thread> uploadingThreads;
// ::size_t uploadSize;
// ::size_t currentPartNo;
// ::size_t fileBytesWritten;
// std::filesystem::path currentFileName;
// std::ofstream output;

// void checkUpload();

// void checkMUPInitialised();

// void uploadPart(std::filesystem::path const &p);

// [[nodiscard]] static std::ofstream makeOutputFile(std::filesystem::path const &p);
#pragma GCC diagnostic ignored "-Wunused-parameter"
S3Sink::S3Sink(std::shared_ptr<Aws::S3::S3Client> s3client, std::string_view s3path, std::size_t const uploadPartSize)
  : client(s3client), bucket(getBucket(s3path)), key(getKey(s3path)), uploadSize(uploadPartSize) {
    SPDLOG_INFO("Creating an S3Sink to {}/{}", bucket, key);
    auto request = Aws::S3::Model::CreateMultipartUploadRequest().WithBucket(bucket).WithKey(key);
    auto result = client->CreateMultipartUpload(request);
}

S3Sink::~S3Sink() noexcept {
    SPDLOG_INFO("Destroying an S3Sink to {}/{}", bucket, key);
}

void S3Sink::host_write(void const *data, std::size_t size) {}

[[noreturn]] void S3Sink::device_write(void const *gpu_data, std::size_t size, rmm::cuda_stream_view stream) {
    throw std::runtime_error("device_write is not supported on S3Sink");
}

[[noreturn]] std::future<void>
  S3Sink::device_write_async(void const *gpu_data, std::size_t size, rmm::cuda_stream_view stream) {
    throw std::runtime_error("device_write is not supported on S3Sink");
}

void S3Sink::flush() {}

void S3Sink::finish() {}

std::size_t S3Sink::bytes_written() noexcept {
    return bytesWritten;
}

}// namespace gpu_compact::cudf_compact::s3