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

package sleeper.ingest.batcher;

import org.apache.commons.lang3.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.validation.BatchIngestMode;
import sleeper.ingest.job.IngestJob;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;

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
        if (store.getPendingFilesOldestFirst().isEmpty()) {
            LOGGER.info("No pending files found");
        } else {
            store.getPendingFilesOldestFirst().stream()
                    .collect(Collectors.groupingBy(FileIngestRequest::getTableName, LinkedHashMap::new, toList()))
                    .forEach((tableName, inputFiles) -> batchTableFiles(tableName, inputFiles, time));
        }
    }

    private void batchTableFiles(String tableName, List<FileIngestRequest> inputFiles, Instant time) {
        TableProperties properties = tablePropertiesProvider.getTableProperties(tableName);
        int minFiles = properties.getInt(INGEST_BATCHER_MIN_JOB_FILES);
        long minBytes = properties.getBytes(INGEST_BATCHER_MIN_JOB_SIZE);
        int maxAgeInSeconds = properties.getInt(INGEST_BATCHER_MAX_FILE_AGE_SECONDS);
        Instant maxReceivedTime = time.minus(Duration.ofSeconds(maxAgeInSeconds));
        long totalBytes = totalBytes(inputFiles);
        boolean maxAgeMet = inputFiles.stream().anyMatch(file -> file.getReceivedTime().isBefore(maxReceivedTime));
        if ((inputFiles.size() >= minFiles &&
                totalBytes >= minBytes)
                || maxAgeMet) {
            if (maxAgeMet) {
                LOGGER.info("At least one file has exceeded the maximum age of {} seconds", maxAgeInSeconds);
            }
            BatchIngestMode batchIngestMode = batchIngestMode(properties).orElse(null);
            LOGGER.info("Creating batches for {} input files with total bytes of {}", inputFiles.size(), totalBytes);
            List<Instant> receivedTimes = inputFiles.stream()
                    .map(FileIngestRequest::getReceivedTime)
                    .sorted().collect(toList());
            LOGGER.info("Files to batch were received between {} and {}",
                    receivedTimes.get(0), receivedTimes.get(receivedTimes.size() - 1));
            createBatches(properties, inputFiles)
                    .forEach(batch -> sendBatch(tableName, batchIngestMode, batch));
        } else {
            if (inputFiles.size() < minFiles) {
                LOGGER.info("Number of input files ({}) does not satisfy minimum file count for job ({})",
                        inputFiles.size(), minFiles);
            }
            if (totalBytes < minBytes) {
                LOGGER.info("Total bytes for input files ({}) does not satisfy minimum size for job ({})",
                        totalBytes, minBytes);
            }
            LOGGER.info("Batching conditions not met, skipping batch creation");
        }
    }

    private void sendBatch(String tableName, BatchIngestMode batchIngestMode, List<FileIngestRequest> batch) {
        String jobId = jobIdSupplier.get();
        List<String> files = store.assignJobGetAssigned(jobId, batch);
        if (files.isEmpty()) {
            LOGGER.error("Not sending job, no files were successfully assigned");
            return;
        }
        IngestJob job = IngestJob.builder()
                .id(jobId)
                .tableName(tableName)
                .files(files)
                .build();
        try {
            String jobQueueUrl = jobQueueUrl(batchIngestMode);
            if (jobQueueUrl == null) {
                LOGGER.error("Discarding created job with no queue configured for table {}: {}", tableName, job);
            } else {
                LOGGER.info("Sending ingest job with {} files to {}", job.getFiles().size(), batchIngestMode);
                queueClient.send(jobQueueUrl, job);
            }
        } catch (RuntimeException e) {
            LOGGER.error("Failed sending job: {}", job, e);
        }
    }

    public static Optional<BatchIngestMode> batchIngestMode(TableProperties properties) {
        return Optional.ofNullable(properties.get(INGEST_BATCHER_INGEST_MODE))
                .map(mode -> EnumUtils.getEnumIgnoreCase(BatchIngestMode.class, mode));
    }

    private String jobQueueUrl(BatchIngestMode batchIngestMode) {
        return Optional.ofNullable(batchIngestMode)
                .map(IngestBatcher::jobQueueUrlProperty)
                .map(instanceProperties::get)
                .orElse(null);
    }

    private static InstanceProperty jobQueueUrlProperty(BatchIngestMode mode) {
        switch (mode) {
            case STANDARD_INGEST:
                return INGEST_JOB_QUEUE_URL;
            case BULK_IMPORT_EMR:
                return BULK_IMPORT_EMR_JOB_QUEUE_URL;
            case BULK_IMPORT_PERSISTENT_EMR:
                return BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
            case BULK_IMPORT_EKS:
                return BULK_IMPORT_EKS_JOB_QUEUE_URL;
            default:
                return null;
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
