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
#include "s3_writer.hpp"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <algorithm> //sort
#include <cstddef>
#include <cudf/utilities/error.hpp>
#include <filesystem>
#include <future>
#include <iostream>
#include <mutex>
#include <ostream>
#include <rmm/cuda_stream_view.hpp>
#include <string>

#include "s3_utils.hpp"
#include "temp_file.hpp"

namespace s3m = Aws::S3::Model;
namespace fs = std::filesystem;

S3Sink::S3Sink(Aws::S3::S3Client &_client, std::string const &s3path,
               ::size_t const uploadPartSize) :
    client(&_client),
    bucket(getBucket(s3path)), key(getKey(s3path)), uploadId(), completedParts(),
    uploadingThreads(), uploadSize(uploadPartSize), bytesWritten(0), currentPartNo(1),
    fileBytesWritten(0), currentFileName(generateLocalFileName()),
    output(makeOutputFile(currentFileName)) {}

S3Sink::~S3Sink() noexcept {
    std::cerr << "Running destructor for S3Sink\n";
    try {
        finish();
    } catch (...) {
        std::cerr << "Exception thrown from S3Sink destructor.\n";
    }
}

[[nodiscard]] std::ofstream S3Sink::makeOutputFile(fs::path const &p) {
    std::cerr << "Making new temp output file " << p << std::endl;
    return std::ofstream(p, std::ios::binary | std::ios::out);
}

[[nodiscard]] bool S3Sink::supports_device_write() const { return false; }

[[nodiscard]] bool S3Sink::is_device_write_preferred(::size_t) const {
    return supports_device_write();
}

[[noreturn]] void S3Sink::device_write(void const *, ::size_t, rmm::cuda_stream_view) {
    CUDF_FAIL("Device writing not supported.");
}

[[noreturn]] std::future<void> S3Sink::device_write_async(void const *, ::size_t,
                                                          rmm::cuda_stream_view) {
    CUDF_FAIL("Device writing not supported.");
}

void S3Sink::host_write(void const *data, ::size_t size) {
    output.write(static_cast<char const *>(data), static_cast<std::streamsize>(size));
    if (!output) {
        std::cerr << "An error has occurred writing to " << currentPartNo << std::endl;
        std::abort();
    }
    bytesWritten += size;
    fileBytesWritten += size;
    checkUpload();
}

void S3Sink::checkUpload() {
    // should we upload a new chunk to S3?
    if (fileBytesWritten >= uploadSize) {
        std::cerr << "Now have " << fileBytesWritten << " ready to be uploaded.\n";
        fs::path oldPath = currentFileName;
        // reset the output file
        currentFileName = generateLocalFileName();
        output.close();
        output = makeOutputFile(currentFileName);
        // upload the current one
        uploadPart(oldPath);
        currentPartNo++;
        fileBytesWritten = 0;
    }
}

void S3Sink::checkMUPInitialised() {
    // make request
    if (uploadId.empty()) {
        s3m::CreateMultipartUploadRequest req{};
        req.WithBucket(bucket).WithKey(key);
        auto outcome = client->CreateMultipartUpload(req);
        std::cerr << "Starting multipart upload to s3://" << bucket << "/" << key << std::flush;
        if (!outcome.IsSuccess()) {
            std::cerr << "Failed: " << outcome.GetError().GetExceptionName() << ": "
                      << outcome.GetError().GetMessage() << std::endl;
        }
        std::cerr << " done\n";
        uploadId = outcome.GetResult().GetUploadId();
    }
}

void S3Sink::uploadPart(fs::path const &fileName) {
    static std::mutex completedMutex{};
    // check to see if the Multipart upload has already been started.
    checkMUPInitialised();
    // lambda for uploading
    auto uploadLambda = [&](fs::path const uploadfileName, ::size_t const partNo,
                            ::size_t const bytesWrittenUpload) {
        // create upload part
        s3m::UploadPartRequest req{};
        req.WithBucket(bucket).WithKey(key).WithUploadId(uploadId).WithPartNumber(
            static_cast<int>(partNo));
        req.SetBody(Aws::MakeShared<Aws::FStream>("iostream", uploadfileName,
                                                  std::ios::in | std::ios::binary));
        // do the upload
        std::cerr << "Start upload of part " << uploadfileName << " part " << partNo << " of "
                  << bytesWrittenUpload << " bytes\n";
        auto outcome = client->UploadPart(req);
        if (!outcome.IsSuccess()) {
            std::cerr << "Failed: " << outcome.GetError().GetExceptionName() << ": "
                      << outcome.GetError().GetMessage() << std::endl;
        }
        std::cerr << "Finished S3 upload of part " << partNo << " file " << uploadfileName << "\n";
        // synchronise the update to completed parts
        std::lock_guard lock{completedMutex};
        // note the ETag for this part
        completedParts.push_back(s3m::CompletedPart()
                                     .WithETag(outcome.GetResult().GetETag())
                                     .WithPartNumber(static_cast<int>(partNo)));
    };
    // start an uploading thread
    uploadingThreads.emplace_back(uploadLambda, fileName, currentPartNo, fileBytesWritten);
}

void S3Sink::finish() {
    // there might be pending bytes
    // close the file if open
    if (output.is_open()) {
        output.close();
    }
    // perform last upload
    if (fileBytesWritten > 0) {
        uploadPart(currentFileName);
    }
    std::cerr << "Joining " << uploadingThreads.size() << " uploading threads...\n";
    for (auto &th : uploadingThreads) {
        th.join();
    }
    // the list of completed parts must be in part number order so sort it
    std::sort(completedParts.begin(), completedParts.end(), [&](auto const &p1, auto const &p2) {
        return p1.GetPartNumber() < p2.GetPartNumber();
    });
    // make completion request
    std::cerr << "Completing multipart upload\n";
    s3m::CompleteMultipartUploadRequest req{};
    req.WithBucket(bucket).WithKey(key).WithUploadId(uploadId).WithMultipartUpload(
        s3m::CompletedMultipartUpload{}.WithParts(completedParts));
    auto outcome = client->CompleteMultipartUpload(req);
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed: " << outcome.GetError().GetExceptionName() << ": "
                  << outcome.GetError().GetMessage() << std::endl;
    }
    std::cerr << "Successfully uploaded to s3://" << bucket << "/" << key << std::endl;
}

void S3Sink::flush() { output.flush(); }

::size_t S3Sink::bytes_written() { return bytesWritten; }
