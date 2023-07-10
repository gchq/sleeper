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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.ingest.batcher.FileIngestRequest;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.CommonProperties.FILE_SYSTEM;
import static sleeper.utils.HadoopPathUtils.getRequestPath;
import static sleeper.utils.HadoopPathUtils.streamFiles;

public class FileIngestRequestSerDe {
    private static final Gson GSON = new GsonBuilder().create();

    private FileIngestRequestSerDe() {
    }

    public static List<FileIngestRequest> fromJson(String json, InstanceProperties properties, Configuration conf, Instant receivedTime) {
        Request request = GSON.fromJson(json, Request.class);
        return request.toFileIngestRequests(properties, conf, receivedTime);
    }

    public static String toJson(String bucketName, List<String> keys, String tableName) {
        return GSON.toJson(new Request(bucketName, keys, tableName));
    }


    private static class Request {
        private final List<String> files;
        private final String tableName;

        Request(String bucketName, List<String> keys, String tableName) {
            this.files = keys.stream().map(key -> bucketName + "/" + key).collect(Collectors.toList());
            this.tableName = tableName;
        }

        List<FileIngestRequest> toFileIngestRequests(InstanceProperties properties, Configuration conf, Instant receivedTime) {
            return streamFiles(files, conf, properties.get(FILE_SYSTEM))
                    .map(file -> FileIngestRequest.builder()
                            .file(getRequestPath(file))
                            .fileSizeBytes(file.getLen())
                            .tableName(tableName)
                            .receivedTime(receivedTime)
                            .build())
                    .collect(Collectors.toList());
        }
    }
}
