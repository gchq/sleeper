/*
 * Copyright 2022 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompletedPart.h>

#include <cstddef>
#include <cudf/io/data_sink.hpp>
#include <filesystem>
#include <fstream>
#include <future>
#include <ostream>
#include <rmm/cuda_stream_view.hpp>
#include <string>
#include <thread>

inline constexpr ::size_t DEFAULT_UPLOAD_SIZE = 300 * 1'048'576;

struct S3Sink final : public cudf::io::data_sink {
   private:
    Aws::S3::S3Client* client;
    Aws::String bucket;
    Aws::String key;
    Aws::String uploadId;
    Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;
    Aws::Vector<std::thread> uploadingThreads;
    ::size_t uploadSize;
    ::size_t bytesWritten;
    ::size_t currentPartNo;
    ::size_t fileBytesWritten;
    std::filesystem::path currentFileName;
    std::ofstream output;

    void checkUpload();

    void checkMUPInitialised();

    void uploadPart(std::filesystem::path const & p);

    [[nodiscard]] static std::ofstream makeOutputFile(std::filesystem::path const & p);
   public:
    S3Sink(Aws::S3::S3Client& s3client, std::string const & s3path, ::size_t const uploadPartSize = DEFAULT_UPLOAD_SIZE);
    S3Sink(S3Sink const &) noexcept = default;
    S3Sink & operator=(S3Sink const &) noexcept = default;
    ~S3Sink() noexcept(true) override;

    void host_write(void const* data, ::size_t size) override;

    [[nodiscard]] bool supports_device_write() const override;

    [[nodiscard]] bool is_device_write_preferred(::size_t size) const override;

    [[noreturn]] void device_write(void const* gpu_data, ::size_t size, rmm::cuda_stream_view stream) override;

    [[noreturn]] std::future<void>
    device_write_async(void const* gpu_data, ::size_t size, rmm::cuda_stream_view stream) override;

    void flush() override;

    void finish();

    ::size_t bytes_written() override;
};
