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
#include <aws/core/Aws.h>
#include <aws/core/http/URI.h>
#include <aws/s3/S3Client.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>  //uintmax_t, size_t
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

// enum for the state of each downloader thread
enum class State { RUNNING,
                   PAUSED,
                   COMPLETED };

using s3meta = std::pair<Aws::String, ::uintmax_t>;

using atomic_state = std::atomic<State>;

using atomic_state_p = std::shared_ptr<atomic_state>;

using file_range = std::pair<::uintmax_t, ::uintmax_t>;

// RAII type for creating connection to AWS
class AwsLibrary {
   public:
    AwsLibrary() noexcept {
        options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Warn;
        options.httpOptions.installSigPipeHandler = true; //silence SIGPIPE from killing program
        Aws::InitAPI(options);
    }
    ~AwsLibrary() noexcept {
        Aws::ShutdownAPI(options);
    }

    // type is not copyable or moveable
    AwsLibrary(AwsLibrary const &) = delete;
    AwsLibrary &operator=(AwsLibrary const &) = delete;
    AwsLibrary(AwsLibrary &&) = delete;
    AwsLibrary &operator=(AwsLibrary &&) = delete;

    Aws::S3::S3Client &getS3Client() noexcept {
        if (!s3client) {
            s3client = std::make_unique<Aws::S3::S3Client>();
        }
        return *s3client;
    }

   private:
    Aws::SDKOptions options;
    std::unique_ptr<Aws::S3::S3Client> s3client;
};

Aws::Vector<s3meta> getS3MetaData(Aws::S3::S3Client &client, Aws::String const &bucket,
                                  Aws::String const &prefix, std::string const &extension = "");

std::pair<std::vector<std::string>, std::shared_ptr<std::atomic_size_t>> initialiseFromS3(
    AwsLibrary &library, std::string const &extension, ::size_t chunkSize,
    ::size_t footerSize, std::vector<std::string> inputFiles);

void download(Aws::S3::S3Client &client, Aws::String const bucket, Aws::String const key, file_range const range,
              ::uintmax_t const fSize, std::filesystem::path const destFile, atomic_state_p state,
              std::atomic_size_t &nextChunk,
              std::mutex &monitor, std::condition_variable &waiter);

void parallelDownload(Aws::S3::S3Client &client, ::size_t const chunkSize, Aws::String const bucket,
                      std::shared_ptr<std::vector<s3meta>> keys, std::shared_ptr<std::vector<Aws::String>> localDests,
                      file_range const range, std::shared_ptr<std::atomic_size_t> downloadedAmount = std::make_shared<std::atomic_size_t>());

Aws::String getBucket(std::string const & s3path) noexcept;

Aws::String getKey(std::string const & s3path) noexcept;
