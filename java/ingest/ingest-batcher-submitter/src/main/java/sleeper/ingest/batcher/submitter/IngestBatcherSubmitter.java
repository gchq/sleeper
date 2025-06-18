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

import sleeper.configuration.utils.S3PathUtils;
import sleeper.configuration.utils.S3PathUtils.S3FileDetails;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class IngestBatcherSubmitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcherSubmitter.class);

    private final TableIndex tableIndex;
    private final IngestBatcherStore store;
    private final IngestBatcherSubmitDeadLetterQueue deadLetterQueue;
    private final S3PathUtils s3PathUtils;

    public IngestBatcherSubmitter(TableIndex tableIndex, IngestBatcherStore store,
            IngestBatcherSubmitDeadLetterQueue deadLetterQueue, S3Client s3Client) {
        this.tableIndex = tableIndex;
        this.store = store;
        this.deadLetterQueue = deadLetterQueue;
        this.s3PathUtils = new S3PathUtils(s3Client);
    }

    public void submit(IngestBatcherSubmitRequest request, Instant receivedTime) {
        List<IngestBatcherTrackedFile> files;
        try {
            files = toTrackedFiles(request, receivedTime);
        } catch (FileNotFoundException | NoSuchKeyException e) {
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
        List<IngestBatcherTrackedFile> list = new ArrayList<IngestBatcherTrackedFile>();

        for (String filename : request.files()) {
            try {
                List<S3FileDetails> fileDetails = s3PathUtils.listFilesAsS3FileDetails(filename);
                list.addAll(convertFileDetailsToTrackedFile(fileDetails, tableID, receivedTime));
            } catch (java.io.FileNotFoundException e) {
                return List.of();
            }

        }
        return list;
    }

    private List<IngestBatcherTrackedFile> convertFileDetailsToTrackedFile(List<S3FileDetails> files, String tableID, Instant recievedTime) {
        List<IngestBatcherTrackedFile> trackedFiles = new ArrayList<IngestBatcherTrackedFile>();
        files.forEach(file -> {
            trackedFiles.add(getIndividualFile(file, tableID, recievedTime));
        });
        return trackedFiles;
    }

    private IngestBatcherTrackedFile getIndividualFile(S3FileDetails file, String tableID, Instant receivedTime) {
        return buildTrackedFile(file.getFullFileLocation(), file.fileObject().size(), tableID, receivedTime);
    }

    private IngestBatcherTrackedFile buildTrackedFile(String filename, Long fileSizeBytes, String tableID, Instant receivedTime) {
        return IngestBatcherTrackedFile.builder()
                .file(filename)
                .fileSizeBytes(fileSizeBytes)
                .tableId(tableID)
                .receivedTime(receivedTime)
                .build();
    }
}
