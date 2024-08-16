#include "cudf_compact/s3/s3_sink.hpp"
#include "cudf_compact/s3/s3_utils.hpp"

namespace gpu_compact::cudf_compact::s3 {


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
::size_t bytesWritten;
// ::size_t currentPartNo;
// ::size_t fileBytesWritten;
// std::filesystem::path currentFileName;
// std::ofstream output;

// void checkUpload();

// void checkMUPInitialised();

// void uploadPart(std::filesystem::path const &p);

// [[nodiscard]] static std::ofstream makeOutputFile(std::filesystem::path const &p);

S3Sink::S3Sink(/*std::shared_ptr<Aws::S3::S3Client> s3client,*/ std::string_view s3path,
  std::size_t const uploadPartSize)
  : /*client(s3client),*/ bucket(getBucket(s3path)), key(getKey(s3path)), uploadSize(uploadPartSize) {}

//     void host_write(void const *data, std::size_t size) override;

//     [[nodiscard]] constexpr bool supports_device_write() const override { return false; }

//     [[nodiscard]] constexpr bool is_device_write_preferred(std::size_t) const override { return false; }

//     [[noreturn]] void device_write(void const *gpu_data, std::size_t size, rmm::cuda_stream_view stream) override;

//     [[noreturn]] std::future<void>
//       device_write_async(void const *gpu_data, std::size_t size, rmm::cuda_stream_view stream) override;

//     void flush() override;

//     void finish();

//     std::size_t bytes_written() noexcept override { return bytesWritten; }
// };

}// namespace gpu_compact::cudf_compact::s3