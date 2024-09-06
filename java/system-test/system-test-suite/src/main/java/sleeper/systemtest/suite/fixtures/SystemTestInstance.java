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

package sleeper.systemtest.suite.fixtures;

import sleeper.configuration.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.EmrInstanceArchitecture;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;

import java.util.HashMap;
import java.util.Map;

import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_RECORDS;
import static sleeper.configuration.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CLIENT_TYPE;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_PART_SIZE_BYTES;
import static sleeper.configuration.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS;
import static sleeper.configuration.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.instance.CommonProperty.METRICS_TABLE_BATCH_SIZE;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_X86_MEMORY;
import static sleeper.configuration.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.instance.IngestProperty.MAXIMUM_CONCURRENT_INGEST_TASKS;
import static sleeper.configuration.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration.noSourceBucket;
import static sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration.usingSystemTestDefaults;

public class SystemTestInstance {
    private SystemTestInstance() {
    }

    public static final SystemTestInstanceConfiguration MAIN = usingSystemTestDefaults("main", SystemTestInstance::buildMainConfiguration);
    public static final SystemTestInstanceConfiguration INGEST_PERFORMANCE = usingSystemTestDefaults("ingest", SystemTestInstance::buildIngestPerformanceConfiguration);
    public static final SystemTestInstanceConfiguration COMPACTION_PERFORMANCE = usingSystemTestDefaults("compact", SystemTestInstance::buildCompactionPerformanceConfiguration);
    public static final SystemTestInstanceConfiguration BULK_IMPORT_PERFORMANCE = usingSystemTestDefaults("emr", SystemTestInstance::buildBulkImportPerformanceConfiguration);
    public static final SystemTestInstanceConfiguration INGEST_NO_SOURCE_BUCKET = noSourceBucket("no-src", SystemTestInstance::buildMainConfiguration);
    public static final SystemTestInstanceConfiguration PARALLEL_COMPACTIONS = usingSystemTestDefaults("cpt-pll", SystemTestInstance::buildCompactionInParallelConfiguration);
    public static final SystemTestInstanceConfiguration COMPACTION_ON_EC2 = usingSystemTestDefaults("cpt-ec2", SystemTestInstance::buildCompactionOnEC2Configuration);
    public static final SystemTestInstanceConfiguration COMMITTER_THROUGHPUT = usingSystemTestDefaults("commitr", SystemTestInstance::buildStateStoreCommitterThroughputConfiguration);

    private static final String MAIN_EMR_MASTER_TYPES = "m6i.xlarge,m6a.xlarge,m5.xlarge,m5a.xlarge";
    private static final String MAIN_EMR_EXECUTOR_TYPES = "m6i.4xlarge,m6a.4xlarge,m5.4xlarge,m5a.4xlarge";

    private static DeployInstanceConfiguration buildMainConfiguration() {
        InstanceProperties properties = new InstanceProperties();
        properties.set(LOGGING_LEVEL, "debug");
        properties.set(OPTIONAL_STACKS, "IngestStack,EmrBulkImportStack,EmrServerlessBulkImportStack,IngestBatcherStack," +
                "CompactionStack,GarbageCollectorStack,PartitionSplittingStack,QueryStack,WebSocketQueryStack,TableMetricsStack");
        properties.set(RETAIN_INFRA_AFTER_DESTROY, "false");
        properties.set(FORCE_RELOAD_PROPERTIES, "true");
        properties.set(DEFAULT_DYNAMO_STRONGLY_CONSISTENT_READS, "true");
        properties.set(DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE, EmrInstanceArchitecture.X86_64.toString());
        properties.set(DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES, MAIN_EMR_MASTER_TYPES);
        properties.set(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, MAIN_EMR_EXECUTOR_TYPES);
        properties.set(MAXIMUM_CONCURRENT_INGEST_TASKS, "1");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "1");
        properties.set(COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS, "5");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE, EmrInstanceArchitecture.X86_64.toString());
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES, MAIN_EMR_MASTER_TYPES);
        properties.set(BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES, MAIN_EMR_EXECUTOR_TYPES);
        properties.set(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING, "false");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, "1");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, "1");
        properties.set(METRICS_TABLE_BATCH_SIZE, "2");
        properties.setTags(Map.of(
                "Description", "Sleeper Maven system test main instance",
                "Environment", "DEV",
                "Product", "Sleeper",
                "ApplicationID", "SLEEPER",
                "Project", "SystemTest",
                "SystemTestInstance", "main"));

        TableProperties tableProperties = new TableProperties(properties);
        tableProperties.setSchema(SystemTestSchema.DEFAULT_SCHEMA);
        tableProperties.set(TABLE_NAME, "system-test");
        return DeployInstanceConfiguration.builder()
                .instanceProperties(properties)
                .tableProperties(tableProperties)
                .build();
    }

    private static DeployInstanceConfiguration buildIngestPerformanceConfiguration() {
        DeployInstanceConfiguration configuration = buildMainConfiguration();
        InstanceProperties properties = configuration.getInstanceProperties();
        properties.set(OPTIONAL_STACKS, "IngestStack");
        properties.set(MAXIMUM_CONCURRENT_INGEST_TASKS, "11");
        properties.set(MAXIMUM_CONNECTIONS_TO_S3, "25");
        properties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arrow");
        properties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "async");
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

    private static DeployInstanceConfiguration buildCompactionPerformanceConfiguration() {
        DeployInstanceConfiguration configuration = buildMainConfiguration();
        InstanceProperties properties = configuration.getInstanceProperties();
        properties.set(OPTIONAL_STACKS, "CompactionStack");
        properties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");
        properties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "X86_64");
        properties.set(COMPACTION_TASK_X86_CPU, "1024");
        properties.set(COMPACTION_TASK_X86_MEMORY, "4096");
        properties.set(MAXIMUM_CONNECTIONS_TO_S3, "25");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "10");
        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", "compactionPerformance");
        tags.put("Description", "Sleeper Maven system test compaction performance instance");
        properties.setTags(tags);

        for (TableProperties tableProperties : configuration.getTableProperties()) {
            tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "11");
        }
        return configuration;
    }

    private static DeployInstanceConfiguration buildBulkImportPerformanceConfiguration() {
        DeployInstanceConfiguration configuration = buildMainConfiguration();
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

    private static DeployInstanceConfiguration buildCompactionOnEC2Configuration() {
        DeployInstanceConfiguration configuration = buildMainConfiguration();
        InstanceProperties properties = configuration.getInstanceProperties();
        properties.set(OPTIONAL_STACKS, "CompactionStack");
        properties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");

        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", "compactionOnEc2");
        tags.put("Description", "Sleeper Maven system test compaction on EC2 instance");
        properties.setTags(tags);
        return configuration;
    }

    private static DeployInstanceConfiguration buildCompactionInParallelConfiguration() {
        DeployInstanceConfiguration configuration = buildMainConfiguration();
        InstanceProperties properties = configuration.getInstanceProperties();
        properties.set(OPTIONAL_STACKS, "CompactionStack");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "300");

        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", "compactionInParallel");
        tags.put("Description", "Sleeper Maven system test compaction in parallel");
        properties.setTags(tags);
        return configuration;
    }

    private static DeployInstanceConfiguration buildStateStoreCommitterThroughputConfiguration() {
        DeployInstanceConfiguration configuration = buildMainConfiguration();
        InstanceProperties properties = configuration.getInstanceProperties();

        // At time of writing, setting an empty list would make it use the default value instead. See issue #3089.
        properties.set(OPTIONAL_STACKS, "None");

        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", "stateStoreCommitterThroughput");
        tags.put("Description", "Sleeper Maven system test state store committer throughput");
        properties.setTags(tags);
        return configuration;
    }
}
