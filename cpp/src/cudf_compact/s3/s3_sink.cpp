#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <spdlog/spdlog.h>

#include "cudf_compact/format_helper.hpp"
#include "cudf_compact/s3/s3_sink.hpp"
#include "cudf_compact/s3/s3_utils.hpp"

#include <exception>
#include <memory>

namespace gpu_compact::cudf_compact::s3
{

Aws::String createUploadRequest(Aws::S3::S3Client &client, Aws::String const &bucket, Aws::String const &key) {
    auto const request = Aws::S3::Model::CreateMultipartUploadRequest().WithBucket(bucket).WithKey(key);
    auto result = unwrap(client.CreateMultipartUpload(request));
    return result.GetUploadId();
}

void S3Sink::uploadBuffer() {
    if (buffer.view().size() > 0) {
        SPDLOG_INFO(
          ff("Buffer of {:Ld} bytes reached upload threshold of {:Ld} bytes", buffer.view().size(), uploadSize));
        auto const stream = Aws::MakeShared<Aws::StringStream>("");
        *stream << buffer.view();
        std::stringstream().swap(buffer);
        auto request =
          Aws::S3::Model::UploadPartRequest().WithBucket(bucket).WithKey(key).WithUploadId(uploadId).WithPartNumber(
            partNo);
        request.SetBody(stream);
        Aws::S3::Model::UploadPartResult result = unwrap(client->UploadPart(request));
        eTags.push_back(std::move(result.GetETag()));
        partNo++;
        SPDLOG_INFO(ff("Upload complete"));
    }
}

S3Sink::S3Sink(std::shared_ptr<Aws::S3::S3Client> s3client, std::string_view s3path, std::size_t const uploadPartSize)
  : client(s3client), bucket(getBucket(s3path)), key(getKey(s3path)),
    uploadId(createUploadRequest(*s3client, bucket, key)), eTags(), uploadSize(uploadPartSize), bytesWritten(0),
    partNo(1) {
    SPDLOG_INFO(ff("Creating an S3Sink to {}/{}", bucket, key));
}

S3Sink::~S3Sink() noexcept {
    try {
        finish();
    } catch (std::exception const &e) { SPDLOG_ERROR(ff("Completing upload failed due to {}", e.what())); }
    SPDLOG_INFO(ff("Destroying an S3Sink to {}/{}", bucket, key));
}

void S3Sink::host_write(void const *data, std::size_t size) {
    buffer.write(reinterpret_cast<char const *>(data), static_cast<std::streamsize>(size));
    if (buffer.view().size() > uploadSize) {
        uploadBuffer();
    }
    bytesWritten += size;
}

[[noreturn]] void S3Sink::device_write(void const *, std::size_t, rmm::cuda_stream_view) {
    throw std::runtime_error("device_write is not supported on S3Sink");
}

[[noreturn]] std::future<void> S3Sink::device_write_async(void const *, std::size_t, rmm::cuda_stream_view) {
    throw std::runtime_error("device_write is not supported on S3Sink");
}

void S3Sink::flush() {}

void S3Sink::finish() {
    uploadBuffer();
    SPDLOG_INFO(ff("Completing multipart upload to {}/{}", bucket, key));
    auto parts = Aws::S3::Model::CompletedMultipartUpload();
    for (int uploadPartNo = 1; auto const &tag : eTags) {
        parts.AddParts(Aws::S3::Model::CompletedPart().WithETag(tag).WithPartNumber(uploadPartNo++));
    }
    auto const request = Aws::S3::Model::CompleteMultipartUploadRequest()
                           .WithBucket(bucket)
                           .WithKey(key)
                           .WithUploadId(uploadId)
                           .WithMultipartUpload(parts);
    auto const result = unwrap(client->CompleteMultipartUpload(request));
}

std::size_t S3Sink::bytes_written() noexcept {
    return bytesWritten;
}

}// namespace gpu_compact::cudf_compact::s3