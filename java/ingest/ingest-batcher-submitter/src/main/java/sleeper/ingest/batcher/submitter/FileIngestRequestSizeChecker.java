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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.NumberFormatUtils;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.parquet.utils.HadoopPathUtils;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

public class FileIngestRequestSizeChecker {
    public static final Logger LOGGER = LoggerFactory.getLogger(FileIngestRequestSizeChecker.class);

    private final InstanceProperties properties;
    private final Configuration conf;
    private final TableIndex tableIndex;

    public FileIngestRequestSizeChecker(InstanceProperties properties, Configuration conf, TableIndex tableIndex) {
        this.properties = properties;
        this.conf = conf;
        this.tableIndex = tableIndex;
    }

    public List<IngestBatcherTrackedFile> toFileIngestRequests(IngestBatcherSubmitRequest request, Instant receivedTime) {
        TableStatus table = tableIndex.getTableByName(request.tableName())
                .orElseThrow(() -> TableNotFoundException.withTableName(request.tableName()));
        return HadoopPathUtils.streamFiles(request.files(), conf, properties.get(FILE_SYSTEM))
                .map(file -> {
                    String filePath = HadoopPathUtils.getRequestPath(file);
                    LOGGER.info("Deserialised ingest request for file {} with size {} to table {}",
                            filePath, NumberFormatUtils.formatBytes(file.getLen()), table);
                    return IngestBatcherTrackedFile.builder()
                            .file(filePath)
                            .fileSizeBytes(file.getLen())
                            .tableId(table.getTableUniqueId())
                            .receivedTime(receivedTime)
                            .build();
                })
                .collect(Collectors.toList());
    }

}
