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

import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.time.Instant;
import java.util.List;

public class IngestBatcherSubmitRequestSerDe {
    private static final Gson GSON = new GsonBuilder().create();

    public List<IngestBatcherTrackedFile> fromJson(String json, Instant receivedTime, FileIngestRequestSizeChecker sizeChecker) {
        IngestBatcherSubmitRequest request = GSON.fromJson(json, IngestBatcherSubmitRequest.class);
        return sizeChecker.toFileIngestRequests(request.tableName(), request.files(), receivedTime);
    }

    public static String toJson(String bucketName, List<String> keys, String tableName) {
        return GSON.toJson(new IngestBatcherSubmitRequest(bucketName, keys, tableName));
    }

    public static String toJson(List<String> files, String tableName) {
        return GSON.toJson(new IngestBatcherSubmitRequest(files, tableName));
    }
}
