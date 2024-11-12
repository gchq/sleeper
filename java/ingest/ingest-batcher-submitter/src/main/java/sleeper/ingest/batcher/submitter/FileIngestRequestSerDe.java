/*
 * Copyright 2022-2024 Crown Copyright
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.ingest.batcher.core.FileIngestRequest;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.util.NumberFormatUtils.formatBytes;
import static sleeper.parquet.utils.HadoopPathUtils.getRequestPath;
import static sleeper.parquet.utils.HadoopPathUtils.streamFiles;

public class FileIngestRequestSerDe {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileIngestRequestSerDe.class);
    private static final Gson GSON = new GsonBuilder().create();
    private final InstanceProperties properties;
    private final Configuration conf;
    private final TableIndex tableIndex;

    public FileIngestRequestSerDe(InstanceProperties properties, Configuration conf, TableIndex tableIndex) {
        this.properties = properties;
        this.conf = conf;
        this.tableIndex = tableIndex;
    }

    public List<FileIngestRequest> fromJson(String json, Instant receivedTime) {
        Request request = GSON.fromJson(json, Request.class);
        return request.toFileIngestRequests(properties, conf, receivedTime, tableIndex);
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

        List<FileIngestRequest> toFileIngestRequests(
                InstanceProperties properties, Configuration conf, Instant receivedTime, TableIndex tableIndex) {
            TableStatus table = tableIndex.getTableByName(tableName)
                    .orElseThrow(() -> TableNotFoundException.withTableName(tableName));
            return streamFiles(files, conf, properties.get(FILE_SYSTEM))
                    .map(file -> {
                        String filePath = getRequestPath(file);
                        LOGGER.info("Deserialised ingest request for file {} with size {} to table {}",
                                filePath, formatBytes(file.getLen()), table);
                        return FileIngestRequest.builder()
                                .file(filePath)
                                .fileSizeBytes(file.getLen())
                                .tableId(table.getTableUniqueId())
                                .receivedTime(receivedTime)
                                .build();
                    })
                    .collect(Collectors.toList());
        }
    }
}
