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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.ingest.job.IngestJob;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IngestBatcher {
    private final TablePropertiesProvider tablePropertiesProvider;
    private final Supplier<String> jobIdSupplier;

    private IngestBatcher(Builder builder) {
        tablePropertiesProvider = builder.tablePropertiesProvider;
        jobIdSupplier = builder.jobIdSupplier;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<IngestJob> batchFiles(List<FileIngestRequest> inputFiles) {
        Map<String, List<FileIngestRequest>> filesByTable = inputFiles.stream()
                .collect(Collectors.groupingBy(FileIngestRequest::getTableName));
        return filesByTable.entrySet().stream()
                .flatMap(entry -> batchTableFiles(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private Stream<IngestJob> batchTableFiles(String tableName, List<FileIngestRequest> inputFiles) {
        TableProperties properties = tablePropertiesProvider.getTableProperties(tableName);
        int minFiles = properties.getInt(TableProperty.INGEST_BATCHER_MIN_JOB_FILES);
        if (inputFiles.size() < minFiles) {
            return Stream.empty();
        } else {
            return inputFiles.stream().map(this::batch);
        }
    }

    private IngestJob batch(FileIngestRequest file) {
        return IngestJob.builder()
                .id(jobIdSupplier.get())
                .tableName(file.getTableName())
                .files(file.getPathToFile())
                .build();
    }

    public static final class Builder {
        private TablePropertiesProvider tablePropertiesProvider;
        private Supplier<String> jobIdSupplier;

        private Builder() {
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder jobIdSupplier(Supplier<String> jobIdSupplier) {
            this.jobIdSupplier = jobIdSupplier;
            return this;
        }

        public IngestBatcher build() {
            return new IngestBatcher(this);
        }
    }
}
