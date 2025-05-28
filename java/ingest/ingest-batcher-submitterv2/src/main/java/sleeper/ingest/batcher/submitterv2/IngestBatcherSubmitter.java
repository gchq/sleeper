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
package sleeper.ingest.batcher.submitterv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

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
    private final S3Client s3Client;

    public IngestBatcherSubmitter(TableIndex tableIndex, IngestBatcherStore store,
            IngestBatcherSubmitDeadLetterQueue deadLetterQueue, S3Client s3Client) {
        this.tableIndex = tableIndex;
        this.store = store;
        this.deadLetterQueue = deadLetterQueue;
        this.s3Client = s3Client;
    }

    public void submit(IngestBatcherSubmitRequest request, Instant receivedTime) {
        List<IngestBatcherTrackedFile> files;
        try {
            files = toTrackedFiles(request, receivedTime);
        } catch (NoSuchKeyException e) {
            LOGGER.info("File not found, sending request: {} to dead letter queue", request);
            deadLetterQueue.submit(request);
            return;
        } catch (TableNotFoundException e) {
            LOGGER.info("Table not found, sending request: {} to dead letter queue", request);
            deadLetterQueue.submit(request);
            return;
        } catch (Exception e) {
            LOGGER.info(e.getMessage(), e);
            return;
        }
        files.forEach(store::addFile);
    }

    private List<IngestBatcherTrackedFile> toTrackedFiles(IngestBatcherSubmitRequest request, Instant receivedTime) {
        String tableID = tableIndex.getTableByName(request.tableName())
                .orElseThrow(() -> TableNotFoundException.withTableName(request.tableName())).getTableUniqueId();
        List<IngestBatcherTrackedFile> list = new ArrayList<IngestBatcherTrackedFile>();

        for (String filename : request.files()) {
            if (!filename.contains("/")) {
                list.addAll(getFilesByBucket(filename, tableID, receivedTime));
            } else {
                String[] filenameParts = filename.split("/");
                String bucket = filenameParts[0];
                String fileOrDirName = filenameParts[1];
                if (fileOrDirName.contains(".")) {
                    list.add(getIndividualFile(bucket, fileOrDirName, tableID, receivedTime));
                } else {
                    list.addAll(getFilesByDirectory(bucket, fileOrDirName, tableID, receivedTime));
                }
            }
        }
        return list;
    }

    private List<IngestBatcherTrackedFile> getFilesByBucket(String bucket, String tableID, Instant receivedTime) {
        return getFilesByDirectory(bucket, "", tableID, receivedTime);
    }

    private List<IngestBatcherTrackedFile> getFilesByDirectory(String bucket, String directory, String tableID, Instant receivedTime) {
        List<IngestBatcherTrackedFile> list = new ArrayList<IngestBatcherTrackedFile>();
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(
                ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(directory)
                        .build());

        for (ListObjectsV2Response page : response) {
            page.contents().forEach((S3Object object) -> {
                list.add(getIndividualFile(bucket, object.key(), tableID, receivedTime));
            });
        }
        return list;
    }

    private IngestBatcherTrackedFile getIndividualFile(String bucket, String key, String tableID, Instant receivedTime) {
        return buildTrackedFile(bucket + "/" + key, getFileSizeBites(bucket, key), tableID, receivedTime);
    }

    private Long getFileSizeBites(String bucket, String key) {
        return s3Client.headObject(
                HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build())
                .contentLength();
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
