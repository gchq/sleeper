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

package sleeper.systemtest.suite.fixtures;

import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.systemtest.drivers.instance.SystemTestParameters;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CLIENT_TYPE;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_PART_SIZE_BYTES;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS;
import static sleeper.configuration.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_X86_MEMORY;
import static sleeper.configuration.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.instance.IngestProperty.MAXIMUM_CONCURRENT_INGEST_TASKS;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;

public enum SystemTestInstance {

    MAIN("main", SystemTestInstance::buildMainConfiguration),
    INGEST_PERFORMANCE("ingest", SystemTestInstance::buildIngestPerformanceConfiguration),
    COMPACTION_PERFORMANCE("compaction", SystemTestInstance::buildCompactionPerformanceConfiguration),
    BULK_IMPORT_PERFORMANCE("emr", SystemTestInstance::buildBulkImportPerformanceConfiguration);

    private final String identifier;
    private final Function<SystemTestParameters, DeployInstanceConfiguration> instanceConfiguration;

    SystemTestInstance(String identifier, Function<SystemTestParameters, DeployInstanceConfiguration> instanceConfiguration) {
        this.identifier = identifier;
        this.instanceConfiguration = instanceConfiguration;
    }

    public String getIdentifier() {
        return identifier;
    }

    public DeployInstanceConfiguration getInstanceConfiguration(SystemTestParameters parameters) {
        return instanceConfiguration.apply(parameters);
    }

    private static DeployInstanceConfiguration buildMainConfiguration(SystemTestParameters parameters) {
        InstanceProperties properties = new InstanceProperties();
        properties.set(LOGGING_LEVEL, "debug");
        properties.set(OPTIONAL_STACKS, "" +
                "IngestStack,EmrBulkImportStack,EmrServerlessBulkImportStack,IngestBatcherStack," +
                "CompactionStack,GarbageCollectorStack,PartitionSplittingStack,QueryStack");
        properties.set(RETAIN_INFRA_AFTER_DESTROY, "false");
        properties.set(FORCE_RELOAD_PROPERTIES, "true");
        properties.set(MAXIMUM_CONCURRENT_INGEST_TASKS, "1");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "1");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING, "false");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, "1");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, "1");
        properties.setTags(Map.of(
                "Description", "Sleeper Maven system test main instance",
                "Environment", "DEV",
                "Product", "Sleeper",
                "ApplicationID", "SLEEPER",
                "Project", "SystemTest",
                "SystemTestInstance", "main"));

        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.setSchema(SystemTestSchema.DEFAULT_SCHEMA);

        return DeployInstanceConfiguration.builder()
                .instanceProperties(properties)
                .tableProperties(tableProperties)
                .build();
    }

    private static DeployInstanceConfiguration buildIngestPerformanceConfiguration(SystemTestParameters parameters) {
        DeployInstanceConfiguration configuration = buildMainConfiguration(parameters);
        InstanceProperties properties = configuration.getInstanceProperties();
        properties.set(OPTIONAL_STACKS, "IngestStack");
        properties.set(MAXIMUM_CONCURRENT_INGEST_TASKS, "11");
        properties.set(MAXIMUM_CONNECTIONS_TO_S3, "25");
        properties.set(INGEST_RECORD_BATCH_TYPE, "arrow");
        properties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "async");
        properties.set(ARROW_INGEST_WORKING_BUFFER_BYTES, "268435456"); // 256MB
        properties.set(ARROW_INGEST_BATCH_BUFFER_BYTES, "1073741824"); // 1GB
        properties.set(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, "2147483648"); // 2GB
        properties.set(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS, "1024");
        properties.set(ASYNC_INGEST_CLIENT_TYPE, "crt");
        properties.set(ASYNC_INGEST_CRT_PART_SIZE_BYTES, "134217728"); // 128MB
        properties.set(ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS, "10");
        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", "ingestPerformance");
        tags.put("Description", "Sleeper Maven system test ingest performance instance");
        properties.setTags(tags);
        return configuration;
    }

    private static DeployInstanceConfiguration buildCompactionPerformanceConfiguration(SystemTestParameters parameters) {
        DeployInstanceConfiguration configuration = buildMainConfiguration(parameters);
        InstanceProperties properties = configuration.getInstanceProperties();
        properties.set(OPTIONAL_STACKS, "CompactionStack");
        properties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "X86_64");
        properties.set(COMPACTION_TASK_X86_CPU, "1024");
        properties.set(COMPACTION_TASK_X86_MEMORY, "4096");
        properties.set(MAXIMUM_CONNECTIONS_TO_S3, "25");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "10");
        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", "compactionPerformance");
        tags.put("Description", "Sleeper Maven system test compaction performance instance");
        properties.setTags(tags);

        TableProperties tableProperties = configuration.getTableProperties();
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        return configuration;
    }

    private static DeployInstanceConfiguration buildBulkImportPerformanceConfiguration(SystemTestParameters parameters) {
        DeployInstanceConfiguration configuration = buildMainConfiguration(parameters);
        InstanceProperties properties = configuration.getInstanceProperties();
        properties.set(OPTIONAL_STACKS, "EmrBulkImportStack");
        properties.set(DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY, "5");
        properties.set(MAXIMUM_CONNECTIONS_TO_S3, "25");
        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", "bulkImportPerformance");
        tags.put("Description", "Sleeper Maven system test bulk import performance instance");
        properties.setTags(tags);
        return configuration;
    }
}
