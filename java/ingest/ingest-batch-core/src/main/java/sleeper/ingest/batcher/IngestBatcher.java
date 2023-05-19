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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.job.IngestJob;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class IngestBatcher {
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final Supplier<String> jobIdSupplier;
    private final IngestBatcherStateStore store;
    private final IngestBatcherQueueClient queueClient;

    private IngestBatcher(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        jobIdSupplier = builder.jobIdSupplier;
        store = builder.store;
        queueClient = builder.queueClient;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void batchFiles() {
        store.getAllFiles().stream()
                .collect(Collectors.groupingBy(FileIngestRequest::getTableName))
                .forEach(this::batchTableFiles);
    }

    private void batchTableFiles(String tableName, List<FileIngestRequest> inputFiles) {
        TableProperties properties = tablePropertiesProvider.getTableProperties(tableName);
        int minFiles = properties.getInt(INGEST_BATCHER_MIN_JOB_FILES);
        long minBytes = properties.getBytes(INGEST_BATCHER_MIN_JOB_SIZE);
        if (inputFiles.size() >= minFiles &&
                totalBytes(inputFiles) >= minBytes) {
            BatchSender batchSender = new BatchSender(properties);
            inputFiles.forEach(batchSender::add);
            batchSender.finish();
        }
    }

    private class BatchSender {
        private final int maxFiles;
        private final long maxBytes;
        private final String tableName;
        private final List<FileIngestRequest> batch = new ArrayList<>();
        private long batchBytes;

        BatchSender(TableProperties properties) {
            maxFiles = properties.getInt(INGEST_BATCHER_MAX_JOB_FILES);
            maxBytes = properties.getBytes(INGEST_BATCHER_MAX_JOB_SIZE);
            tableName = properties.get(TABLE_NAME);
        }

        void add(FileIngestRequest file) {
            batchBytes += file.getFileSizeBytes();
            batch.add(file);
            if (batch.size() >= maxFiles || batchBytes >= maxBytes) {
                sendJob();
                batchBytes = 0;
                batch.clear();
            }
        }

        void finish() {
            if (!batch.isEmpty()) {
                sendJob();
            }
        }

        void sendJob() {
            IngestJob job = IngestJob.builder()
                    .id(jobIdSupplier.get())
                    .tableName(tableName)
                    .files(batch.stream()
                            .map(FileIngestRequest::getPathToFile)
                            .collect(Collectors.toList()))
                    .build();
            store.assignJob(job.getId(), batch);
            queueClient.send(instanceProperties.get(INGEST_JOB_QUEUE_URL), job);
        }
    }

    private static long totalBytes(List<FileIngestRequest> files) {
        return files.stream().mapToLong(FileIngestRequest::getFileSizeBytes).sum();
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;
        private Supplier<String> jobIdSupplier;
        private IngestBatcherStateStore store;
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

        public Builder store(IngestBatcherStateStore store) {
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
