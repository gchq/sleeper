#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>
#include <cudf/io/data_sink.hpp>
#include <rmm/cuda_stream_view.hpp>

#include <atomic>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <string_view>

namespace gpu_compact::cudf_compact::s3
{

inline constexpr std::size_t DEFAULT_UPLOAD_SIZE = 256 * 1'048'576;

struct S3Sink final : public cudf::io::data_sink
{
  private:
    std::shared_ptr<Aws::S3::S3Client> client;
    Aws::String bucket;
    Aws::String key;
    Aws::String uploadId;
    Aws::Vector<Aws::S3::Model::CompletedPart> eTags;
    std::atomic_size_t activeUploadCount;
    std::mutex tagsLock;
    std::size_t uploadSize = 0;
    std::size_t bytesWritten;
    int partNo;
    Aws::StringStream buffer;

    void uploadBuffer();
    void finish();

  public:
    S3Sink(std::shared_ptr<Aws::S3::S3Client> s3client,
      std::string_view s3path,
      std::size_t const uploadPartSize = DEFAULT_UPLOAD_SIZE);

    virtual ~S3Sink() noexcept override;

    void host_write(void const *data, std::size_t size) override;

    [[nodiscard]] constexpr bool supports_device_write() const override {
        return false;
    }

    [[nodiscard]] constexpr bool is_device_write_preferred(std::size_t) const override {
        return false;
    }

    [[noreturn]] void device_write(void const *gpu_data, std::size_t size, rmm::cuda_stream_view stream) override;

    [[noreturn]] std::future<void>
      device_write_async(void const *gpu_data, std::size_t size, rmm::cuda_stream_view stream) override;

    void flush() override;

    std::size_t bytes_written() noexcept override;
};

}// namespace gpu_compact::cudf_compact::s3
