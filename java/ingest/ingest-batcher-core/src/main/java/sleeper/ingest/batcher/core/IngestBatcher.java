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

package sleeper.ingest.batcher.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.validation.IngestQueue;
import sleeper.core.table.TableStatus;
import sleeper.ingest.core.job.IngestJob;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.core.util.NumberFormatUtils.formatBytes;

public class IngestBatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBatcher.class);
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final Supplier<String> jobIdSupplier;
    private final Supplier<Instant> timeSupplier;
    private final IngestBatcherStore store;
    private final IngestBatcherQueueClient queueClient;

    private IngestBatcher(Builder builder) {
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        tablePropertiesProvider = Objects.requireNonNull(builder.tablePropertiesProvider, "tablePropertiesProvider must not be null");
        jobIdSupplier = Objects.requireNonNull(builder.jobIdSupplier, "jobIdSupplier must not be null");
        timeSupplier = Objects.requireNonNull(builder.timeSupplier, "timeSupplier must not be null");
        store = Objects.requireNonNull(builder.store, "store must not be null");
        queueClient = Objects.requireNonNull(builder.queueClient, "queueClient must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public void batchFiles() {
        Instant time = timeSupplier.get();
        LOGGER.info("Requesting pending files from IngestBatcherStore");
        List<FileIngestRequest> pendingFiles = store.getPendingFilesOldestFirst();
        if (pendingFiles.isEmpty()) {
            LOGGER.info("No pending files found");
        } else {
            LOGGER.info("Found {} pending files", pendingFiles.size());
            pendingFiles.stream()
                    .collect(Collectors.groupingBy(FileIngestRequest::getTableId, LinkedHashMap::new, toList()))
                    .forEach((tableId, inputFiles) -> batchTableFiles(tableId, inputFiles, time));
        }
    }

    private void batchTableFiles(String tableId, List<FileIngestRequest> inputFiles, Instant time) {
        long totalBytes = totalBytes(inputFiles);
        TableProperties properties = tablePropertiesProvider.getById(tableId);
        TableStatus table = properties.getStatus();
        LOGGER.info("Attempting to batch {} files of total size {} for table {}",
                inputFiles.size(), formatBytes(totalBytes), table);
        if (shouldCreateBatches(properties, inputFiles, time)) {
            IngestQueue ingestQueue = properties.getEnumValue(INGEST_BATCHER_INGEST_QUEUE, IngestQueue.class);
            LOGGER.info("Creating batches for {} files with total size of {} for table {}",
                    inputFiles.size(), formatBytes(totalBytes), table);
            List<Instant> receivedTimes = inputFiles.stream()
                    .map(FileIngestRequest::getReceivedTime)
                    .sorted().collect(toList());
            LOGGER.info("Files to batch were received between {} and {}",
                    receivedTimes.get(0), receivedTimes.get(receivedTimes.size() - 1));
            createBatches(properties, inputFiles)
                    .forEach(batch -> sendBatch(table, ingestQueue, batch));
        }
    }

    private boolean shouldCreateBatches(
            TableProperties properties, List<FileIngestRequest> inputFiles, Instant time) {
        int minFiles = properties.getInt(INGEST_BATCHER_MIN_JOB_FILES);
        long minBytes = properties.getBytes(INGEST_BATCHER_MIN_JOB_SIZE);
        int maxAgeInSeconds = properties.getInt(INGEST_BATCHER_MAX_FILE_AGE_SECONDS);
        Instant maxReceivedTime = time.minus(Duration.ofSeconds(maxAgeInSeconds));
        long totalBytes = totalBytes(inputFiles);
        boolean maxAgeMet = inputFiles.stream().anyMatch(file -> file.getReceivedTime().isBefore(maxReceivedTime));
        if (maxAgeMet) {
            LOGGER.info("At least one file has reached the maximum age of {} seconds", maxAgeInSeconds);
            return true;
        } else {
            LOGGER.info("No files have reached the maximum age of {} seconds", maxAgeInSeconds);
        }
        boolean meetsMinFiles = true;
        if (inputFiles.size() < minFiles) {
            LOGGER.info("Number of files ({}) does not satisfy the minimum file count for a job ({})",
                    inputFiles.size(), minFiles);
            meetsMinFiles = false;
        } else {
            LOGGER.info("Number of files ({}) satisfies the minimum file count for a job ({})",
                    inputFiles.size(), minFiles);
        }
        if (totalBytes < minBytes) {
            LOGGER.info("Total size for files {} does not satisfy the minimum size for a job {}",
                    formatBytes(totalBytes), formatBytes(minBytes));
            meetsMinFiles = false;
        } else {
            LOGGER.info("Total size for files {} satisfies the minimum size for a job {}",
                    formatBytes(totalBytes), formatBytes(minBytes));
        }
        return meetsMinFiles;
    }

    private void sendBatch(TableStatus table, IngestQueue ingestQueue, List<FileIngestRequest> batch) {
        String jobId = jobIdSupplier.get();
        List<String> files = store.assignJobGetAssigned(jobId, batch);
        if (files.isEmpty()) {
            LOGGER.error("Not sending job, no files were successfully assigned");
            return;
        }
        long totalBytes = totalBytes(batch);
        IngestJob job = IngestJob.builder()
                .id(jobId)
                .tableId(table.getTableUniqueId())
                .files(files)
                .build();
        try {
            String jobQueueUrl = ingestQueue.getJobQueueUrl(instanceProperties);
            if (jobQueueUrl == null) {
                LOGGER.error("Discarding created job with no queue configured for table {}: {}", table, job);
            } else {
                LOGGER.info("Sending ingest job of id {} with {} files and total size of {} to {}",
                        jobId, job.getFiles().size(), formatBytes(totalBytes), ingestQueue);
                queueClient.send(jobQueueUrl, job);
            }
        } catch (RuntimeException e) {
            LOGGER.error("Failed sending job: {}", job, e);
        }
    }

    private static Stream<List<FileIngestRequest>> createBatches(
            TableProperties properties, List<FileIngestRequest> inputFiles) {
        BatchCreator batchCreator = new BatchCreator(properties);
        inputFiles.forEach(batchCreator::add);
        return batchCreator.streamBatches();
    }

    private static class BatchCreator {
        private final int maxFiles;
        private final long maxBytes;
        private final List<Batch> batches = new ArrayList<>();

        BatchCreator(TableProperties properties) {
            maxFiles = properties.getInt(INGEST_BATCHER_MAX_JOB_FILES);
            maxBytes = properties.getBytes(INGEST_BATCHER_MAX_JOB_SIZE);
        }

        void add(FileIngestRequest file) {
            getBatchWithSpaceFor(file).add(file);
        }

        Batch getBatchWithSpaceFor(FileIngestRequest file) {
            return batches.stream()
                    .filter(batch -> batch.hasSpaceForFile(file))
                    .findFirst().orElseGet(() -> {
                        Batch batch = new Batch(maxFiles, maxBytes);
                        batches.add(batch);
                        return batch;
                    });
        }

        Stream<List<FileIngestRequest>> streamBatches() {
            return batches.stream().map(Batch::getFiles);
        }
    }

    private static class Batch {
        private final List<FileIngestRequest> files = new ArrayList<>();
        private final int maxBatchSizeInFiles;
        private long batchSpaceInBytes;

        Batch(int maxBatchSizeInFiles, long maxBatchSizeInBytes) {
            this.maxBatchSizeInFiles = maxBatchSizeInFiles;
            this.batchSpaceInBytes = maxBatchSizeInBytes;
        }

        boolean hasSpaceForFile(FileIngestRequest file) {
            return file.getFileSizeBytes() <= batchSpaceInBytes
                    && files.size() < maxBatchSizeInFiles;
        }

        void add(FileIngestRequest file) {
            files.add(file);
            batchSpaceInBytes -= file.getFileSizeBytes();
        }

        List<FileIngestRequest> getFiles() {
            return files;
        }
    }

    private static long totalBytes(List<FileIngestRequest> files) {
        return files.stream().mapToLong(FileIngestRequest::getFileSizeBytes).sum();
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;
        private Supplier<String> jobIdSupplier = () -> UUID.randomUUID().toString();
        private Supplier<Instant> timeSupplier = Instant::now;
        private IngestBatcherStore store;
        private IngestBatcherQueueClient queueClient;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder jobIdSupplier(Supplier<String> jobIdSupplier) {
            this.jobIdSupplier = jobIdSupplier;
            return this;
        }

        public Builder timeSupplier(Supplier<Instant> timeSupplier) {
            this.timeSupplier = timeSupplier;
            return this;
        }

        public Builder store(IngestBatcherStore store) {
            this.store = store;
            return this;
        }

        public Builder queueClient(IngestBatcherQueueClient queueClient) {
            this.queueClient = queueClient;
            return this;
        }

        public IngestBatcher build() {
            return new IngestBatcher(this);
        }
    }
}
