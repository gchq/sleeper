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

package sleeper.systemtest.suite.fixtures;

import sleeper.core.SleeperVersion;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.model.EmrInstanceArchitecture;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.model.StateStoreCommitterPlatform;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;
import sleeper.systemtest.dsl.util.SystemTestSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static sleeper.core.deploy.SleeperScheduleRule.COMPACTION_TASK_CREATION;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_ROWS;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.core.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CLIENT_TYPE;
import static sleeper.core.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_PART_SIZE_BYTES;
import static sleeper.core.properties.instance.AsyncIngestPartitionFileWriterProperty.ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS;
import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_INFRA_AFTER_DESTROY;
import static sleeper.core.properties.instance.CommonProperty.RETAIN_LOGS_AFTER_DESTROY;
import static sleeper.core.properties.instance.CommonProperty.USER_JARS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_CPU;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_MEMORY;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_X86_MEMORY;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.core.properties.instance.CompactionProperty.DEFAULT_COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.core.properties.instance.GarbageCollectionProperty.DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;
import static sleeper.core.properties.instance.IngestProperty.MAXIMUM_CONCURRENT_INGEST_TASKS;
import static sleeper.core.properties.instance.LoggingLevelsProperty.LOGGING_LEVEL;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_TABLE_BATCH_SIZE;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY;
import static sleeper.core.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DATA_ENGINE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_ROW_BATCH_TYPE;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_PLATFORM;
import static sleeper.core.properties.model.OptionalStack.CompactionStack;
import static sleeper.core.properties.model.OptionalStack.EmrBulkImportStack;
import static sleeper.core.properties.model.OptionalStack.EmrServerlessBulkImportStack;
import static sleeper.core.properties.model.OptionalStack.GarbageCollectorStack;
import static sleeper.core.properties.model.OptionalStack.IngestBatcherStack;
import static sleeper.core.properties.model.OptionalStack.IngestStack;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration.usingSystemTestDefaults;

public class SystemTestInstance {
    private SystemTestInstance() {
    }

    public static final SystemTestInstanceConfiguration MAIN = usingSystemTestDefaults("main", SystemTestInstance::createMainConfiguration);
    public static final SystemTestInstanceConfiguration INGEST_PERFORMANCE = usingSystemTestDefaults("ingest", SystemTestInstance::createIngestPerformanceConfiguration);
    public static final SystemTestInstanceConfiguration COMPACTION_PERFORMANCE = usingSystemTestDefaults("cptprf", SystemTestInstance::createCompactionPerformanceConfiguration);
    public static final SystemTestInstanceConfiguration COMPACTION_PERFORMANCE_DATAFUSION = usingSystemTestDefaults("cpt-df", SystemTestInstance::createCompactionPerformanceOnDataFusionConfiguration);
    public static final SystemTestInstanceConfiguration COMPACTION_CREATION = SystemTestInstanceConfiguration.builder()
            .shortName("cpt-cr")
            .deployConfig(SystemTestInstance::createCompactionCreationConfiguration)
            .disableSchedules(Set.of(COMPACTION_TASK_CREATION))
            .build();
    public static final SystemTestInstanceConfiguration BULK_IMPORT_PERFORMANCE = usingSystemTestDefaults("emr", SystemTestInstance::createBulkImportPerformanceConfiguration);
    public static final SystemTestInstanceConfiguration BULK_IMPORT_EKS = usingSystemTestDefaults("bi-eks", SystemTestInstance::createBulkImportOnEksConfiguration);
    public static final SystemTestInstanceConfiguration BULK_IMPORT_PERSISTENT_EMR = usingSystemTestDefaults("emrpst", SystemTestInstance::createBulkImportOnPersistentEmrConfiguration);
    public static final SystemTestInstanceConfiguration PARALLEL_COMPACTIONS = usingSystemTestDefaults("cptpll", SystemTestInstance::createCompactionInParallelConfiguration);
    public static final SystemTestInstanceConfiguration COMPACTION_ON_EC2 = usingSystemTestDefaults("cptec2", SystemTestInstance::createCompactionOnEC2Configuration);
    public static final SystemTestInstanceConfiguration COMMITTER_THROUGHPUT = usingSystemTestDefaults("cmmitr", SystemTestInstance::createStateStoreCommitterThroughputConfiguration);
    public static final SystemTestInstanceConfiguration ECS_STATESTORE = usingSystemTestDefaults("ecsss", SystemTestInstance::createEcsStatestoreConfiguration);
    public static final SystemTestInstanceConfiguration REENABLE_OPTIONAL_STACKS = usingSystemTestDefaults("opstck", SystemTestInstance::createReenableOptionalStacksConfiguration);
    public static final SystemTestInstanceConfiguration OPTIONAL_FEATURES_DISABLED = SystemTestInstanceConfiguration.builder()
            .shortName("xf-off")
            .deployConfig(SystemTestInstance::createOptionalFeaturesDisabledConfiguration)
            .useSystemTestIngestSourceBucket(false)
            .build();

    // These instance types are set to give EMR more options when creating a cluster, and avoid problems due to
    // insufficient resource availability. This is not used during performance tests.
    private static final String MAIN_EMR_MASTER_TYPES = "m7i.xlarge,m6i.xlarge,m6a.xlarge,m5.xlarge,m5a.xlarge";
    private static final String MAIN_EMR_EXECUTOR_TYPES = "m7i.4xlarge,m6i.4xlarge,m6a.4xlarge,m5.4xlarge,m5a.4xlarge";

    private static InstanceProperties createInstancePropertiesWithDefaults() {
        InstanceProperties properties = new InstanceProperties();
        properties.set(LOGGING_LEVEL, "debug");
        properties.set(RETAIN_INFRA_AFTER_DESTROY, "false");
        properties.set(RETAIN_LOGS_AFTER_DESTROY, "true");
        properties.set(FORCE_RELOAD_PROPERTIES, "true");
        properties.setTags(Map.of(
                "Environment", "DEV",
                "Product", "Sleeper",
                "ApplicationID", "SLEEPER",
                "Project", "SystemTest"));
        return properties;
    }

    private static InstanceProperties createInstanceProperties() {
        InstanceProperties properties = createInstancePropertiesWithDefaults();
        properties.set(DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE, EmrInstanceArchitecture.X86_64.toString());
        properties.set(DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES, MAIN_EMR_MASTER_TYPES);
        properties.set(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, MAIN_EMR_EXECUTOR_TYPES);
        properties.set(MAXIMUM_CONCURRENT_INGEST_TASKS, "1");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "1");
        properties.set(COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS, "5");
        properties.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 1);
        properties.set(BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE, EmrInstanceArchitecture.X86_64.toString());
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES, MAIN_EMR_MASTER_TYPES);
        properties.set(BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES, MAIN_EMR_EXECUTOR_TYPES);
        properties.set(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING, "false");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY, "1");
        properties.set(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY, "2");
        properties.set(METRICS_TABLE_BATCH_SIZE, "2");
        properties.setNumber(DEFAULT_GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 1);
        properties.setNumber(GARBAGE_COLLECTOR_PERIOD_IN_MINUTES, 1);
        properties.set(USER_JARS, "example-iterators-" + SleeperVersion.getVersion() + ".jar");
        return properties;
    }

    private static SleeperInstanceConfiguration createMainConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setEnumList(OPTIONAL_STACKS, OptionalStack.SYSTEM_TEST_STACKS);
        setSystemTestTags(properties, "main", "Sleeper Maven system test main instance");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createIngestPerformanceConfiguration() {
        InstanceProperties properties = createInstancePropertiesWithDefaults();
        properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
        properties.set(MAXIMUM_CONCURRENT_INGEST_TASKS, "11");
        properties.set(DEFAULT_INGEST_ROW_BATCH_TYPE, "arrow");
        properties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "async");
        properties.set(ARROW_INGEST_WORKING_BUFFER_BYTES, "268435456"); // 256MB
        properties.set(ARROW_INGEST_BATCH_BUFFER_BYTES, "1073741824"); // 1GB
        properties.set(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, "2147483648"); // 2GB
        properties.set(ARROW_INGEST_MAX_SINGLE_WRITE_TO_FILE_ROWS, "1024");
        properties.set(ASYNC_INGEST_CLIENT_TYPE, "crt");
        properties.set(ASYNC_INGEST_CRT_PART_SIZE_BYTES, "134217728"); // 128MB
        properties.set(ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS, "10");
        setSystemTestTags(properties, "ingestPerformance", "Sleeper Maven system test ingest performance");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createCompactionPerformanceConfiguration() {
        InstanceProperties properties = createInstancePropertiesWithDefaults();
        properties.setEnumList(OPTIONAL_STACKS,
                // Enable GC to reduce the number of files needing deletion during teardown
                List.of(OptionalStack.CompactionStack, OptionalStack.GarbageCollectorStack));
        properties.setEnum(DEFAULT_DATA_ENGINE, DataEngine.JAVA);
        properties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");
        properties.set(COMPACTION_EC2_TYPE, "t3.xlarge");
        properties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "X86_64");
        properties.set(COMPACTION_TASK_X86_CPU, "1024");
        properties.set(COMPACTION_TASK_X86_MEMORY, "4096");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "10");
        properties.set(DEFAULT_COMPACTION_FILES_BATCH_SIZE, "11");
        setSystemTestTags(properties, "compactionPerformance", "Sleeper Maven system test compaction performance");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createCompactionPerformanceOnDataFusionConfiguration() {
        InstanceProperties properties = createInstancePropertiesWithDefaults();
        properties.setEnumList(OPTIONAL_STACKS,
                // Enable GC to reduce the number of files needing deletion during teardown
                List.of(OptionalStack.CompactionStack, OptionalStack.GarbageCollectorStack, OptionalStack.WebSocketQueryStack));
        properties.setEnum(DEFAULT_DATA_ENGINE, DataEngine.DATAFUSION);
        properties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");
        properties.set(COMPACTION_EC2_TYPE, "t4g.xlarge");
        properties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "ARM64");
        properties.set(COMPACTION_TASK_ARM_CPU, "4096");
        properties.set(COMPACTION_TASK_ARM_MEMORY, "8192");
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "10");
        properties.set(DEFAULT_COMPACTION_FILES_BATCH_SIZE, "11");
        setSystemTestTags(properties, "compactionOnDataFusion", "Sleeper Maven system test compaction performance on DataFusion");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createCompactionCreationConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);
        setSystemTestTags(properties, "compactionCreation", "Sleeper Maven system test compaction creation");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createBulkImportPerformanceConfiguration() {
        InstanceProperties properties = createInstancePropertiesWithDefaults();
        properties.setEnum(OPTIONAL_STACKS, OptionalStack.EmrBulkImportStack);
        properties.set(DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY, "5");
        setSystemTestTags(properties, "bulkImportPerformance", "Sleeper Maven system test bulk import performance");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createBulkImportOnEksConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setList(OPTIONAL_STACKS, List.of());
        setSystemTestTags(properties, "bulkImportOnEks", "Sleeper Maven system test bulk import on EKS");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createBulkImportOnPersistentEmrConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setList(OPTIONAL_STACKS, List.of());
        setSystemTestTags(properties, "bulkImportOnPersistentEmr", "Sleeper Maven system test bulk import on persistent EMR cluster");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createCompactionOnEC2Configuration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);
        properties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");
        setSystemTestTags(properties, "compactionOnEc2", "Sleeper Maven system test compaction on EC2");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createCompactionInParallelConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setEnumList(OPTIONAL_STACKS,
                // Enable GC to reduce the number of files needing deletion during teardown
                List.of(OptionalStack.CompactionStack, OptionalStack.GarbageCollectorStack));
        properties.set(MAXIMUM_CONCURRENT_COMPACTION_TASKS, "200");
        properties.unset(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS); // Use default
        setSystemTestTags(properties, "compactionInParallel", "Sleeper Maven system test compaction in parallel");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createStateStoreCommitterThroughputConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setList(OPTIONAL_STACKS, List.of());
        setSystemTestTags(properties, "stateStoreCommitterThroughput", "Sleeper Maven system test state store committer throughput");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createEcsStatestoreConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setList(OPTIONAL_STACKS, List.of());
        properties.set(STATESTORE_COMMITTER_PLATFORM, StateStoreCommitterPlatform.EC2.toString());
        setSystemTestTags(properties, "ecsStatestore", "Sleeper Maven system test instance with ECS state store");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createReenableOptionalStacksConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setList(OPTIONAL_STACKS, List.of());
        setSystemTestTags(properties, "reenableOptionalStacks", "Sleeper Maven system test reenable optional stacks");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createOptionalFeaturesDisabledConfiguration() {
        InstanceProperties properties = createInstanceProperties();
        properties.setEnumList(OPTIONAL_STACKS, List.of(IngestStack, CompactionStack, GarbageCollectorStack,
                EmrBulkImportStack, EmrServerlessBulkImportStack, IngestBatcherStack));
        properties.set(COMPACTION_TRACKER_ENABLED, "false");
        properties.set(INGEST_TRACKER_ENABLED, "false");
        setSystemTestTags(properties, "optionalFeaturesDisabled", "Sleeper Maven system test optional features disabled");
        return createInstanceConfiguration(properties);
    }

    private static SleeperInstanceConfiguration createInstanceConfiguration(InstanceProperties instanceProperties) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(SystemTestSchema.DEFAULT_SCHEMA);
        tableProperties.set(TABLE_NAME, "system-test");
        return SleeperInstanceConfiguration.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties)
                .build();
    }

    private static void setSystemTestTags(InstanceProperties properties, String instanceName, String description) {
        Map<String, String> tags = new HashMap<>(properties.getTags());
        tags.put("SystemTestInstance", instanceName);
        tags.put("Description", description);
        properties.setTags(tags);
    }
}
