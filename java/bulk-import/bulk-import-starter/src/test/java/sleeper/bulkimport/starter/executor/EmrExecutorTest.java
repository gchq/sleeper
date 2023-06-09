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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimits;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimitsUnitType;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ManagedScalingPolicy;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.rejectedRun;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

class EmrExecutorTest {
    private AmazonElasticMapReduce emr;
    private TablePropertiesProvider tablePropertiesProvider;
    private AtomicReference<RunJobFlowRequest> requested;
    private AmazonS3 amazonS3;
    private StateStoreProvider stateStoreProvider;
    private InstanceProperties instanceProperties;
    private IngestJobStatusStore ingestJobStatusStore;

    @BeforeEach
    public void setUpEmr() {
        requested = new AtomicReference<>();
        amazonS3 = mock(AmazonS3.class);
        emr = mock(AmazonElasticMapReduce.class);
        when(emr.runJobFlow(any(RunJobFlowRequest.class)))
                .then((Answer<RunJobFlowResult>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return new RunJobFlowResult();
                });
        instanceProperties = new InstanceProperties();
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        tablePropertiesProvider = mock(TablePropertiesProvider.class);
        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> new TableProperties(instanceProperties));
        stateStoreProvider = mock(StateStoreProvider.class);
        when(stateStoreProvider.getStateStore(any())).thenReturn(
                inMemoryStateStoreWithFixedSinglePartition(schemaWithKey("key")));
        ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
    }

    @Test
    void shouldCreateAClusterOfThreeMachinesByDefault() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        EmrExecutor emrExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        emrExecutor.runJob(myJob);

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        Integer instanceCount = config.getInstanceGroups().stream()
                .map(InstanceGroupConfig::getInstanceCount)
                .reduce(Integer::sum)
                .orElseThrow(IllegalArgumentException::new);
        assertThat(instanceCount).isEqualTo(3);
    }

    @Test
    void shouldUseInstanceTypeDefinedInJob() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        EmrExecutor emrExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .platformSpec(ImmutableMap.of(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
                .build();

        // When
        emrExecutor.runJob(myJob);

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorInstanceType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getInstanceType).findFirst().orElse("not-found");
        assertThat(executorInstanceType).isEqualTo("r5.xlarge");
    }

    @Test
    void shouldUseDefaultMarketType() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        EmrExecutor emrExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        emrExecutor.runJob(myJob);

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorMarketType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getMarket).findFirst().orElse("not-found");
        assertThat(executorMarketType).isEqualTo("SPOT");
    }

    @Test
    void shouldUseMarketTypeDefinedInConfig() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        EmrExecutor emrExecutor = createExecutorWithDefaults();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
        tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS, "10");
        tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> tableProperties);


        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        emrExecutor.runJob(myJob);

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorMarketType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getMarket).findFirst().orElse("not-found");
        assertThat(executorMarketType).isEqualTo("ON_DEMAND");
    }

    @Test
    void shouldUseMarketTypeDefinedInRequest() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        EmrExecutor emrExecutor = createExecutorWithDefaults();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
        tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS, "10");
        tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> tableProperties);

        Map<String, String> platformSpec = new HashMap<>();
        platformSpec.put(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE.getPropertyName(), "SPOT");

        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .platformSpec(platformSpec)
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When
        emrExecutor.runJob(myJob);

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorMarketType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getMarket).findFirst().orElse("not-found");
        assertThat(executorMarketType).isEqualTo("SPOT");
    }

    @Test
    void shouldEnableEMRManagedClusterScaling() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        EmrExecutor emrExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .platformSpec(ImmutableMap.of(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
                .build();

        // When
        emrExecutor.runJob(myJob);

        // Then
        ManagedScalingPolicy scalingPolicy = requested.get().getManagedScalingPolicy();
        assertThat(scalingPolicy).extracting(ManagedScalingPolicy::getComputeLimits)
                .extracting(ComputeLimits::getMaximumCapacityUnits, ComputeLimits::getUnitType)
                .containsExactly(10, ComputeLimitsUnitType.Instances.name());
    }

    @Test
    void shouldUseUserProvidedConfigIfValuesOverrideDefaults() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        EmrExecutor emrExecutor = createExecutorWithDefaults();
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .sparkConf(ImmutableMap.of("spark.hadoop.fs.s3a.connection.maximum", "100"))
                .platformSpec(ImmutableMap.of(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
                .build();

        // When
        emrExecutor.runJob(myJob);

        // Then
        List<String> args = requested.get().getSteps().get(0).getHadoopJarStep().getArgs();
        Map<String, String> conf = new HashMap<>();
        for (int i = 0; i < args.size(); i++) {
            if ("--conf".equalsIgnoreCase(args.get(i))) {
                String[] confArg = args.get(i + 1).split("=");
                conf.put(confArg[0], confArg[1]);
            }
        }

        assertThat(conf).containsEntry("spark.hadoop.fs.s3a.connection.maximum", "100");
    }

    @Test
    void shouldNotCreateClusterIfMinimumPartitionCountNotReached() {
        // Given
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        when(tablePropertiesProvider.getTableProperties(any()))
                .thenAnswer((Answer<TableProperties>) x -> {
                    TableProperties tableProperties = new TableProperties(instanceProperties);
                    tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "5");
                    return tableProperties;
                });

        EmrExecutor emrExecutor = createExecutor(
                "test-task", () -> Instant.parse("2023-06-02T15:41:00Z"));
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .build();

        // When / Then
        emrExecutor.runJob(myJob);
        assertThat(ingestJobStatusStore.getAllJobs("myTable"))
                .containsExactly(jobStatus(myJob.toIngestJob(),
                        rejectedRun(myJob.toIngestJob(), "test-task", Instant.parse("2023-06-02T15:41:00Z"),
                                "The minimum partition count was not reached")));
    }

    private EmrExecutor createExecutorWithDefaults() {
        return ExecutorFactory.createEmrExecutor(emr, instanceProperties,
                tablePropertiesProvider, stateStoreProvider, ingestJobStatusStore, amazonS3);
    }

    private EmrExecutor createExecutor(String taskId, Supplier<Instant> validationTimeSupplier) {
        return new EmrExecutor(emr, instanceProperties, tablePropertiesProvider,
                stateStoreProvider, ingestJobStatusStore, amazonS3, taskId, validationTimeSupplier);
    }
}
