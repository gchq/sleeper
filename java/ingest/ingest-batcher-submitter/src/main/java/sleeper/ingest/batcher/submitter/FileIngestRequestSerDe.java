/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.ingest.batcher.FileIngestRequest;

import java.time.Instant;

public class FileIngestRequestSerDe {
    private static final Gson GSON = new GsonBuilder().create();

    private FileIngestRequestSerDe() {
    }

    public static FileIngestRequest fromJson(String json, Instant receivedTime) {
        Request request = GSON.fromJson(json, Request.class);
        return FileIngestRequest.builder()
                .pathToFile(request.pathToFile)
                .fileSizeBytes(request.fileSizeBytes)
                .tableName(request.tableName)
                .receivedTime(receivedTime)
                .build();
    }

    public static String toJson(AmazonS3 s3, String bucketName, String key, String tableName) {
        long fileSizeBytes = s3.getObjectMetadata(bucketName, key).getContentLength();
        return GSON.toJson(new Request(bucketName + "/" + key, fileSizeBytes, tableName));
    }


    private static class Request {
        private final String pathToFile;
        private final long fileSizeBytes;
        private final String tableName;

        Request(String pathToFile, long fileSizeBytes, String tableName) {
            this.pathToFile = pathToFile;
            this.fileSizeBytes = fileSizeBytes;
            this.tableName = tableName;
        }
    }
}
