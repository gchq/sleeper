#include "io/s3_sink.hpp"

#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <spdlog/spdlog.h>

#include "format_helper/format_helper.hpp"
#include "io/s3_utils.hpp"

#include <algorithm>
#include <exception>
#include <functional>
#include <ios>
#include <iostream>

namespace gpu_compact::s3
{

Aws::String createUploadRequest(Aws::S3::S3Client &client, Aws::String const &bucket, Aws::String const &key) {
    auto const request = Aws::S3::Model::CreateMultipartUploadRequest().WithBucket(bucket).WithKey(key);
    auto result = unwrap(client.CreateMultipartUpload(request));
    return result.GetUploadId();
}

void S3Sink::uploadBuffer() {
    if (buffer.view().size() > 0) {
        SPDLOG_DEBUG(ff("Uploading {:Ld} bytes in part number {:Ld}", buffer.view().size(), partNo));
        auto const stream = Aws::MakeShared<Aws::StringStream>("");
        stream->swap(buffer);
        auto request =
          Aws::S3::Model::UploadPartRequest().WithBucket(bucket).WithKey(key).WithUploadId(uploadId).WithPartNumber(
            partNo);
        request.SetBody(stream);
        activeUploadCount++;
        client->UploadPartAsync(request,
          [this, startedPartNo = partNo](Aws::S3::S3Client const *,
            Aws::S3::Model::UploadPartRequest const &,
            Aws::S3::Model::UploadPartOutcome const &response,
            std::shared_ptr<const Aws::Client::AsyncCallerContext> const &) {
              SPDLOG_DEBUG(ff("Part number {:Ld} waiting 5 seconds..."));
              auto const result = unwrap(response);
              {
                  std::lock_guard<std::mutex> guard{ tagsLock };
                  eTags.push_back(std::move(Aws::S3::Model::CompletedPart()
                                              .WithPartNumber(startedPartNo)
                                              .WithETag(std::move(result.GetETag()))));
              }
              activeUploadCount--;
              activeUploadCount.notify_all();
              SPDLOG_DEBUG(ff("Finished uploading part number {:Ld}", startedPartNo));
          });
        partNo++;
    }// endif
}

S3Sink::S3Sink(std::shared_ptr<Aws::S3::S3Client> s3client, std::string_view s3path, std::size_t const uploadPartSize)
  : client(s3client), bucket(getBucket(s3path)), key(getKey(s3path)),
    uploadId(createUploadRequest(*s3client, bucket, key)), eTags(), activeUploadCount(0), tagsLock(),
    uploadSize(uploadPartSize), bytesWritten(0), partNo(1) {
    SPDLOG_DEBUG(ff("Creating an S3Sink to {}/{}", bucket, key));
}

S3Sink::~S3Sink() noexcept {
    try {
        finish();
    } catch (std::exception const &e) { SPDLOG_ERROR(ff("Completing upload failed due to {}", e.what())); }
    std::cout << "S3Sink going away now.\n";
    SPDLOG_DEBUG(ff("Destroying an S3Sink to {}/{}", bucket, key));
}

void S3Sink::host_write(void const *data, std::size_t size) {
    buffer.write(reinterpret_cast<char const *>(data), static_cast<std::streamsize>(size));
    if (buffer.view().size() > uploadSize) {
        SPDLOG_DEBUG(
          ff("Upload buffer of {:Ld} bytes exceeded upload size of {:Ld} bytes", buffer.view().size(), uploadSize));
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
    SPDLOG_DEBUG(ff("Finishing multipart load"));
    uploadBuffer();
    SPDLOG_DEBUG(ff("Awaiting completion of uploads..."));
    std::size_t count;
    while ((count = activeUploadCount.load()) != 0) {
        SPDLOG_DEBUG(ff("Awaiting {:Ld} pending uploads", count));
        activeUploadCount.wait(count);
    }
    SPDLOG_DEBUG(ff("Completing multipart upload"));
    auto parts = Aws::S3::Model::CompletedMultipartUpload();
    {
        std::lock_guard<std::mutex> guard{ tagsLock };
        std::sort(eTags.begin(), eTags.end(), [cmp = std::less{}](auto const &a, auto const &b) constexpr noexcept {
            return cmp(a.GetPartNumber(), b.GetPartNumber());
        });
        parts.SetParts(eTags);
    }
    auto const request = Aws::S3::Model::CompleteMultipartUploadRequest()
                           .WithBucket(bucket)
                           .WithKey(key)
                           .WithUploadId(uploadId)
                           .WithMultipartUpload(parts);
    auto const result = unwrap(client->CompleteMultipartUpload(request));
    SPDLOG_DEBUG(ff("Multipart upload completed"));
}

std::size_t S3Sink::bytes_written() noexcept {
    return bytesWritten;
}

}// namespace gpu_compact::s3