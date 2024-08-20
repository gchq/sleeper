#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <spdlog/spdlog.h>

#include "cudf_compact/s3/s3_sink.hpp"
#include "cudf_compact/s3/s3_utils.hpp"

#include <exception>
#include <memory>
#include <sstream>

namespace gpu_compact::cudf_compact::s3
{

Aws::String createUpload(Aws::S3::S3Client &client, Aws::String const &bucket, Aws::String const &key) {
    auto const request = Aws::S3::Model::CreateMultipartUploadRequest().WithBucket(bucket).WithKey(key);
    auto result = unwrap(client.CreateMultipartUpload(request));
    return result.GetUploadId();
}

S3Sink::S3Sink(std::shared_ptr<Aws::S3::S3Client> s3client, std::string_view s3path, std::size_t const uploadPartSize)
  : client(s3client), bucket(getBucket(s3path)), key(getKey(s3path)), uploadId(createUpload(*s3client, bucket, key)),
    eTags(), uploadSize(uploadPartSize), bytesWritten(0), partNo(1) {
    SPDLOG_INFO("Creating an S3Sink to {}/{}", bucket, key);
}

S3Sink::~S3Sink() noexcept {
    SPDLOG_INFO("Destroying an S3Sink to {}/{}", bucket, key);
}

void S3Sink::host_write(void const *data, std::size_t size) {
    std::shared_ptr stream = Aws::MakeShared<std::stringstream>("global");
    stream->write(reinterpret_cast<char const *>(data), static_cast<std::streamsize>(size));
    stream->flush();
    SPDLOG_INFO("Start upload of {:Ld} bytes, Actually got {:Ld} bytes", size, stream->str().size());
    auto request =
      Aws::S3::Model::UploadPartRequest().WithBucket(bucket).WithKey(key).WithUploadId(uploadId).WithPartNumber(partNo);
    request.SetBody(stream);
    Aws::S3::Model::UploadPartResult result = unwrap(client->UploadPart(request));
    eTags.push_back(std::move(result.GetETag()));
    partNo++;
    SPDLOG_INFO("Just uploaded {:Ld} bytes", size);
    bytesWritten += size;
}

[[noreturn]] void S3Sink::device_write(void const *, std::size_t, rmm::cuda_stream_view) {
    throw std::runtime_error("device_write is not supported on S3Sink");
}

[[noreturn]] std::future<void> S3Sink::device_write_async(void const *, std::size_t, rmm::cuda_stream_view) {
    throw std::runtime_error("device_write is not supported on S3Sink");
}

void S3Sink::flush() {}

void S3Sink::finish() {}

std::size_t S3Sink::bytes_written() noexcept {
    return bytesWritten;
}

}// namespace gpu_compact::cudf_compact::s3