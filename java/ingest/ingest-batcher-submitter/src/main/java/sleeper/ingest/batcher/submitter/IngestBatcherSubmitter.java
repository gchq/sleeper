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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import sleeper.configuration.utils.S3ExpandDirectories;
import sleeper.configuration.utils.S3FileNotFoundException;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.time.Instant;
import java.util.List;

public class IngestBatcherSubmitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherSubmitter.class);

    private final TableIndex tableIndex;
    private final IngestBatcherStore store;
    private final IngestBatcherSubmitDeadLetterQueue deadLetterQueue;
    private final S3ExpandDirectories expandDirectories;

    public IngestBatcherSubmitter(TableIndex tableIndex, IngestBatcherStore store,
            IngestBatcherSubmitDeadLetterQueue deadLetterQueue, S3Client s3Client) {
        this.tableIndex = tableIndex;
        this.store = store;
        this.deadLetterQueue = deadLetterQueue;
        this.expandDirectories = new S3ExpandDirectories(s3Client);
    }

    public void submit(IngestBatcherSubmitRequest request, Instant receivedTime) {
        List<IngestBatcherTrackedFile> files;
        try {
            files = toTrackedFiles(request, receivedTime);
        } catch (S3FileNotFoundException | NoSuchKeyException e) {
            LOGGER.info("File not found, sending request to dead letter queue: {}", request, e);
            deadLetterQueue.submit(request);
            return;
        } catch (TableNotFoundException e) {
            LOGGER.info("Table not found, sending request to dead letter queue: {}", request);
            deadLetterQueue.submit(request);
            return;
        }
        files.forEach(store::addFile);
        LOGGER.info("Added {} files to the ingest batcher store", files.size());
    }

    private List<IngestBatcherTrackedFile> toTrackedFiles(IngestBatcherSubmitRequest request, Instant receivedTime) {
        String tableID = tableIndex.getTableByName(request.tableName())
                .orElseThrow(() -> TableNotFoundException.withTableName(request.tableName())).getTableUniqueId();
        return expandDirectories.expandPaths(request.files())
                .streamFilesThrowIfAnyPathIsEmpty()
                .map(file -> IngestBatcherTrackedFile.builder()
                        .file(file.pathForJob())
                        .fileSizeBytes(file.fileSizeBytes())
                        .tableId(tableID)
                        .receivedTime(receivedTime)
                        .build())
                .toList();
    }
}
