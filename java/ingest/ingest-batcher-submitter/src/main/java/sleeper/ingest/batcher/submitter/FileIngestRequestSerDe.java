/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.ingest.batcher.submitter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.ingest.batcher.core.FileIngestRequest;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class FileIngestRequestSerDe {
    private static final Gson GSON = new GsonBuilder().create();

    public List<FileIngestRequest> fromJson(String json, Instant receivedTime, FileIngestRequestSizeChecker sizeChecker) {
        Request request = GSON.fromJson(json, Request.class);
        return sizeChecker.toFileIngestRequests(request.tableName, request.files, receivedTime);
    }

    public static String toJson(String bucketName, List<String> keys, String tableName) {
        return GSON.toJson(new Request(bucketName, keys, tableName));
    }

    public static String toJson(List<String> files, String tableName) {
        return GSON.toJson(new Request(files, tableName));
    }

    private static class Request {
        private final List<String> files;
        private final String tableName;

        Request(String bucketName, List<String> keys, String tableName) {
            this(keys.stream().map(key -> bucketName + "/" + key).collect(Collectors.toList()), tableName);
        }

        Request(List<String> files, String tableName) {
            this.files = files;
            this.tableName = tableName;
        }
    }
}
