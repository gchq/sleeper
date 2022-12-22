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
#include "s3_utils.hpp"

#include <aws/core/Aws.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>

#include <algorithm>  //std::all_of
#include <atomic>
#include <chrono>  //std::chrono::milliseconds
#include <condition_variable>
#include <cstdint>  //uintmax_t
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "string_helpers.hpp"
#include "temp_file.hpp"

namespace s3m = Aws::S3::Model;
namespace fs = std::filesystem;

Aws::Vector<s3meta> getS3MetaData(Aws::S3::S3Client &client, Aws::String const &bucket,
                                  Aws::String const &prefix, std::string const &extension) {
    s3m::ListObjectsV2Request req{};
    req.WithBucket(bucket)
        .WithPrefix(prefix);
    Aws::Vector<s3meta> results{};

    bool moreData = true;
    while (moreData) {
        // start sending requests
        auto outcome = client.ListObjectsV2(req);
        if (outcome.IsSuccess()) {
            s3m::ListObjectsV2Result &result = outcome.GetResult();
            for (auto const &ob : result.GetContents()) {
                // only add if not a "folder" prefix and the extension matches
                if (ob.GetSize() && endsWith(ob.GetKey(), extension)) {
                    results.emplace_back(ob.GetKey(), ob.GetSize());
                }
            }
            // check continuation token
            moreData = result.GetIsTruncated();
            if (moreData) {
                req.SetContinuationToken(result.GetNextContinuationToken());
            }
        } else {
            std::cerr << "Failed: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
            std::string error(outcome.GetError().GetMessage().c_str(), outcome.GetError().GetMessage().size());
            throw std::runtime_error(error);
        }
    }
    return results;
}

static void createLocalFile(Aws::String const &fileName, ::uintmax_t const size) {
    // touch the file to destroy and overwrite
    Aws::FStream touchFile(std::string(fileName), std::ios::out | std::ios::binary);
    // make file of right size
    std::filesystem::resize_file(fileName, size);
}

static void downloadFilePart(Aws::S3::S3Client &client,
                             Aws::String const bucket,
                             Aws::String const key,
                             file_range const range,
                             fs::path const destFile,
                             atomic_state_p state,
                             std::shared_ptr<std::atomic_size_t> nextChunk,
                             std::mutex &monitor,
                             std::condition_variable &waiter) {
    using namespace std::string_literals;
    std::string byteRange = "bytes="s + std::to_string(range.first) + "-"s + std::to_string(range.second);
    Aws::String byteRangeCopy(byteRange.c_str(), byteRange.size());
    std::cerr << "Start download s3://" << bucket << "/" << key << " " << byteRange << " to " << destFile << std::endl;
    s3m::GetObjectRequest request{};
    ::size_t bytesTransferred = 0;
    request.WithBucket(bucket)
        .WithKey(key)
        .WithRange(byteRangeCopy)
        .SetDataReceivedEventHandler(
            [&](auto, auto, long long amount) {
                bytesTransferred += static_cast<::size_t>(amount);
                // should we sleep?
                if (bytesTransferred >= *nextChunk) {
                    // we should sleep
                    std::cerr << key << " is pausing at " << bytesTransferred << " bytes\n";
                    std::unique_lock lock{monitor};
                    state->store(State::PAUSED);
                    waiter.wait(lock, [&] { return bytesTransferred < *nextChunk; });
                    state->store(State::RUNNING);
                    std::cerr << key << " is continuing\n";
                }
            });
    request.SetResponseStreamFactory([&] {
        // this file is destructed by the S3 API once request is finished.
        Aws::FStream *filePointer = Aws::New<Aws::FStream>("S3Object", destFile,
                                                           std::ios::in | std::ios::out | std::ios::binary);
        filePointer->seekp(static_cast<Aws::FStream::off_type>(range.first), std::ios::beg);
        return filePointer;
    });

    // make request
    auto outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed: " << outcome.GetError().GetExceptionName() << ": " << outcome.GetError().GetMessage() << std::endl;
    }
    state->store(State::COMPLETED);
    std::cerr << "Finish download s3://" << bucket << "/" << key << " (" << outcome.GetResult().GetContentLength() << ")\n";
}

std::pair<std::vector<std::string>, std::shared_ptr<std::atomic_size_t>> initialiseFromS3(
    AwsLibrary &library, std::string const &extension, ::size_t chunkSize,
    ::size_t footerSize, std::vector<std::string> inputFiles) {
    Aws::S3::S3Client &client = library.getS3Client();
    std::shared_ptr keys = std::make_shared<std::vector<s3meta>>();
    std::shared_ptr localFiles = std::make_shared<std::vector<Aws::String>>();
    ::size_t maxSize = 0;
    // TODO: stop assuming all objects are in same bucket
    Aws::String bucket;
    for (auto const &f : inputFiles) {
        bucket = getBucket(f);
        auto key = getKey(f);
        auto s3Objects = getS3MetaData(client, bucket, key, extension);
        for (auto const &ob : s3Objects) {
            // generate local file name
            localFiles->emplace_back(generateLocalFileName().native());
            keys->push_back(ob);
            // create the local file and size it correctly
            createLocalFile(localFiles->back(), ob.second);
            maxSize = std::max(maxSize, ob.second);
        }
    }
    parallelDownload(client, chunkSize, bucket, keys, localFiles, {footerSize, 0});
    std::shared_ptr downloadedAmount = std::make_shared<std::atomic_size_t>();
    auto thread = std::thread(parallelDownload, std::ref(client), chunkSize, bucket, keys,
                              localFiles, file_range{0, maxSize}, downloadedAmount);
    thread.detach();

    // copy the vector of strings out and convert back to std::string
    std::vector<std::string> resultFiles{};
    for (auto const &aws_string : *localFiles) {
        resultFiles.emplace_back(aws_string.c_str(), aws_string.size());
    }
    return std::pair{std::vector{resultFiles}, downloadedAmount};
}

void parallelDownload(Aws::S3::S3Client &client, ::size_t const chunkSize, Aws::String const bucket,
                      std::shared_ptr<std::vector<s3meta>> keys, std::shared_ptr<std::vector<Aws::String>> localDests,
                      file_range const range, std::shared_ptr<std::atomic_size_t> downloadedAmount) {
    if (localDests->size() != keys->size()) {
        throw std::invalid_argument("number of local destination files differs from number of remote S3 objects");
    }

    *downloadedAmount = 0;
    std::shared_ptr<std::atomic_size_t> nextPausePoint = std::make_shared<std::atomic_size_t>(chunkSize);
    std::vector<std::pair<std::thread, atomic_state_p>> threads{};
    /* We can't use atomics in a vector easily since std::atomic is not copy constructible,
     * so we have to put them in smart pointers. Shared ptr means we can copy them easily
     *into the threads.*/
    std::mutex monitor{};
    std::condition_variable waiter{};

    // start all transfers asynchronously
    for (::size_t i = 0; i < keys->size(); ++i) {
        s3meta const &obMeta = (*keys)[i];
        Aws::String const &destination = (*localDests)[i];
        // figure out destination path
        // make a thread for this and give it an atomic state smart pointer
        atomic_state_p nextState = std::make_shared<atomic_state>(State::RUNNING);
        // are doing a footer download? TODO: Find a better range setting.
        // currently, if the end of the range is zero, then the first number is taken as the
        // number of bytes to grab from the end of the file
        auto effectiveRange = range;
        if (range.second == 0) {
            effectiveRange = {obMeta.second - range.first, obMeta.second - 1};
        }
        threads.emplace_back(std::thread{downloadFilePart, std::ref(client), bucket, obMeta.first, effectiveRange,
                                         destination, nextState, nextPausePoint, std::ref(monitor), std::ref(waiter)},
                             nextState);
    }
    auto allComplete = [&] {
        return std::all_of(threads.cbegin(), threads.cend(), [](auto const &p) {
            State s = p.second->load();
            return s == State::COMPLETED;
        });
    };
    auto allBlockedOrComplete = [&] {
        return std::all_of(threads.cbegin(), threads.cend(), [](auto const &p) {
            State s = p.second->load();
            return (s == State::PAUSED) || (s == State::COMPLETED);
        });
    };

    /* Loop waiting for all threads to complete their work. When all threads complete,
     * we are done. But once they are all blocked or complete, it means we must increase
     * the threshold they are working to.*/
    while (!allComplete()) {
        if (allBlockedOrComplete()) {
            // acquire lock and increase the data limit
            std::lock_guard guard{monitor};
            *nextPausePoint += chunkSize;
            *downloadedAmount += chunkSize;
            std::cerr << "Next chunk limit " << *nextPausePoint << std::endl;
            // notify all blocked threads
            waiter.notify_all();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    // signal everything downloaded
    *downloadedAmount = range.second;
    // reclaim all threads
    for (auto &pair : threads) {
        pair.first.join();
    }
    std::cerr << "Downloads complete!\n";
}

Aws::String getBucket(std::string const &s3path) noexcept {
    Aws::Http::URI s3uri(std::string{s3path}.c_str());
    return s3uri.GetAuthority();
}

Aws::String getKey(std::string const &s3path) noexcept {
    Aws::Http::URI s3uri(s3path.c_str());
    auto key = s3uri.GetPath();
    // remove leading slash
    if (!key.empty()) {
        key.erase(0, 1);
    }
    return key;
}