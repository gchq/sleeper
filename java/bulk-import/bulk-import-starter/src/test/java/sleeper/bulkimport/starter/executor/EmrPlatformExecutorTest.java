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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimits;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimitsUnitType;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.acceptedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.rejectedRun;

class EmrPlatformExecutorTest {
    private final AmazonElasticMapReduce emr = mock(AmazonElasticMapReduce.class);
    private final AtomicReference<RunJobFlowRequest> requested = new AtomicReference<>();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final InMemoryIngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();

    @BeforeEach
    public void setUpEmr() {
        when(emr.runJobFlow(any(RunJobFlowRequest.class)))
                .then((Answer<RunJobFlowResult>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return new RunJobFlowResult();
                });
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        instanceProperties.set(SUBNETS, "subnet-abc");
    }

    @Nested
    @DisplayName("Configure instance groups")
    class ConfigureInstanceGroups {

        @Test
        void shouldCreateAClusterOfThreeMachinesByDefault() {
            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroups())
                    .extracting(InstanceGroupConfig::getInstanceRole, InstanceGroupConfig::getInstanceCount)
                    .containsExactlyInAnyOrder(
                            tuple("MASTER", 1),
                            tuple("CORE", 2));
        }

        @Test
        void shouldUseInstanceTypeDefinedInJob() {
            // Given
            BulkImportJob myJob = singleFileJobBuilder()
                    .platformSpec(ImmutableMap.of(
                            BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES.getPropertyName(),
                            "r5.xlarge"))
                    .build();

            // When
            executorWithInstanceGroups().runJob(myJob);

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.CORE))
                    .extracting(InstanceGroupConfig::getInstanceType)
                    .containsExactly("r5.xlarge");
        }

        @Test
        void shouldUseDefaultMarketType() {
            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.CORE))
                    .extracting(InstanceGroupConfig::getMarket)
                    .containsExactly("SPOT");
        }

        @Test
        void shouldUseMarketTypeDefinedInConfig() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY, "5");
            tableProperties.set(BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY, "10");
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.CORE))
                    .extracting(InstanceGroupConfig::getMarket)
                    .containsExactly("ON_DEMAND");
        }

        @Test
        void shouldUseMarketTypeDefinedInRequest() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY, "5");
            tableProperties.set(BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY, "10");
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

            Map<String, String> platformSpec = new HashMap<>();
            platformSpec.put(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE.getPropertyName(), "SPOT");

            BulkImportJob myJob = singleFileJobBuilder()
                    .platformSpec(platformSpec).build();

            // When
            executorWithInstanceGroups().runJob(myJob);

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.CORE))
                    .extracting(InstanceGroupConfig::getMarket)
                    .containsExactly("SPOT");
        }

        @Test
        void shouldSetSingleSubnetForInstanceGroup() {
            // Given
            instanceProperties.set(SUBNETS, "test-subnet");

            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroupSubnetId()).isEqualTo("test-subnet");
        }

        @Test
        void shouldSetRandomSubnetForInstanceGroupWhenMultipleSubnetsSpecified() {
            // Given
            instanceProperties.set(SUBNETS, "test-subnet-1,test-subnet-2,test-subnet-3");
            int randomSubnetIndex = 1;

            // When
            executorWithInstanceGroupsSubnetIndexPicker(numSubnets -> randomSubnetIndex)
                    .runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroupSubnetId()).isEqualTo("test-subnet-2");
        }

        @Test
        void shouldUseFirstInstanceTypeForExecutorsWhenMoreThanOneIsSpecified() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, "m5.4xlarge,m5a.4xlarge");

            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.CORE))
                    .extracting(InstanceGroupConfig::getInstanceType)
                    .containsExactly("m5.4xlarge");
        }

        @Test
        void shouldUseFirstInstanceTypeForDriverWhenMoreThanOneIsSpecified() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES, "m5.xlarge,m5a.xlarge");

            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.MASTER))
                    .extracting(InstanceGroupConfig::getInstanceType)
                    .containsExactly("m5.xlarge");
        }

        @Test
        void shouldSetComputeLimits() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY, "5");

            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedComputeLimits())
                    .isEqualTo(new ComputeLimits()
                            .withUnitType(ComputeLimitsUnitType.Instances)
                            .withMinimumCapacityUnits(1)
                            .withMaximumCapacityUnits(5));
        }
    }

    @Nested
    @DisplayName("Configure instance fleets")
    class ConfigureInstanceFleets {

        @Test
        void shouldCreateAClusterOfThreeMachinesByDefault() {
            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleets())
                    .extracting(InstanceFleetConfig::getInstanceFleetType,
                            InstanceFleetConfig::getTargetOnDemandCapacity, InstanceFleetConfig::getTargetSpotCapacity)
                    .containsExactlyInAnyOrder(
                            tuple("MASTER", 1, null),
                            tuple("CORE", null, 2));
        }

        @Test
        void shouldUseMarketTypeDefinedInConfig() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY, "5");
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleets(InstanceFleetType.CORE))
                    .extracting(InstanceFleetConfig::getTargetOnDemandCapacity, InstanceFleetConfig::getTargetSpotCapacity)
                    .containsExactly(tuple(5, null));
        }

        @Test
        void shouldUseMarketTypeDefinedInRequest() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY, "5");
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

            Map<String, String> platformSpec = new HashMap<>();
            platformSpec.put(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE.getPropertyName(), "SPOT");

            BulkImportJob myJob = singleFileJobBuilder()
                    .platformSpec(platformSpec).build();

            // When
            executorWithInstanceFleets().runJob(myJob);

            // Then
            assertThat(requestedInstanceFleets(InstanceFleetType.CORE))
                    .extracting(InstanceFleetConfig::getTargetOnDemandCapacity, InstanceFleetConfig::getTargetSpotCapacity)
                    .containsExactly(tuple(null, 5));
        }

        @Test
        void shouldSetSubnetsForInstanceFleet() {
            // Given
            instanceProperties.set(SUBNETS, "test-subnet-1,test-subnet-2");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleetSubnetIds())
                    .containsExactly("test-subnet-1", "test-subnet-2");
        }

        @Test
        void shouldUseMultipleInstanceTypesForExecutorsWhenSpecified() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, "m5.4xlarge,m5a.4xlarge");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleets(InstanceFleetType.CORE))
                    .flatExtracting(InstanceFleetConfig::getInstanceTypeConfigs)
                    .extracting(InstanceTypeConfig::getInstanceType)
                    .containsExactly("m5.4xlarge", "m5a.4xlarge");
        }

        @Test
        void shouldUseMultipleInstanceTypesForDriverWhenSpecified() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES, "m5.xlarge,m5a.xlarge");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleets(InstanceFleetType.MASTER))
                    .flatExtracting(InstanceFleetConfig::getInstanceTypeConfigs)
                    .extracting(InstanceTypeConfig::getInstanceType)
                    .containsExactly("m5.xlarge", "m5a.xlarge");
        }

        @Test
        void shouldSetComputeLimits() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY, "3");
            tableProperties.set(BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY, "5");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedComputeLimits())
                    .isEqualTo(new ComputeLimits()
                            .withUnitType(ComputeLimitsUnitType.InstanceFleetUnits)
                            .withMinimumCapacityUnits(3)
                            .withMaximumCapacityUnits(5));
        }

        @Test
        void shouldSetCapacityWeightsForInstanceTypesForExecutorsWhenSpecified() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, "m5.4xlarge,5,m5a.4xlarge");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleets(InstanceFleetType.CORE))
                    .flatExtracting(InstanceFleetConfig::getInstanceTypeConfigs)
                    .extracting(InstanceTypeConfig::getInstanceType, InstanceTypeConfig::getWeightedCapacity)
                    .containsExactly(
                            tuple("m5.4xlarge", 5),
                            tuple("m5a.4xlarge", null));
        }
    }

    @Test
    void shouldUseUserProvidedConfigIfValuesOverrideDefaults() {
        // Given
        BulkImportJob myJob = singleFileJobBuilder()
                .sparkConf(ImmutableMap.of("spark.hadoop.fs.s3a.connection.maximum", "100"))
                .platformSpec(ImmutableMap.of(BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES.getPropertyName(), "r5.xlarge"))
                .build();

        // When
        executor().runJob(myJob);

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
        tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "5");
        BulkImportJob myJob = singleFileJob();
        BulkImportExecutor executor = executorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

        // When
        executor.runJob(myJob);

        // Then
        assertThat(requested.get())
                .isNull();
        assertThat(ingestJobStatusStore.getAllJobs(tableId))
                .containsExactly(jobStatus(myJob.toIngestJob(),
                        rejectedRun(myJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"),
                                "The minimum partition count was not reached")));
    }

    @Test
    void shouldReportJobRunIdToStatusStore() {
        // Given
        BulkImportJob myJob = singleFileJob();
        BulkImportExecutor executor = executorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

        // When
        executor.runJob(myJob, "test-job-run");

        // Then
        assertThat(ingestJobStatusStore.getAllJobs(tableId))
                .containsExactly(jobStatus(myJob.toIngestJob(),
                        acceptedRun(myJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"))));
        assertThat(ingestJobStatusStore.streamTableRecords(tableId))
                .extracting(ProcessStatusUpdateRecord::getJobRunId)
                .containsExactly("test-job-run");
    }

    private BulkImportExecutor executor() {
        return executorWithInstanceConfiguration(new FakeEmrInstanceConfiguration());
    }

    private BulkImportExecutor executorWithInstanceConfiguration(EmrInstanceConfiguration configuration) {
        return executor(configuration, Instant::now);
    }

    private BulkImportExecutor executorWithValidationTime(Instant validationTime) {
        return executor(new FakeEmrInstanceConfiguration(), List.of(validationTime).iterator()::next);
    }

    private BulkImportExecutor executor(
            EmrInstanceConfiguration configuration, Supplier<Instant> validationTimeSupplier) {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        return new BulkImportExecutor(instanceProperties, tablePropertiesProvider,
                new FixedStateStoreProvider(tableProperties,
                        inMemoryStateStoreWithFixedSinglePartition(tableProperties.getSchema())),
                ingestJobStatusStore, (job, jobRunId) -> {
                },
                new EmrPlatformExecutor(emr, instanceProperties, tablePropertiesProvider, configuration),
                validationTimeSupplier);
    }

    private BulkImportExecutor executorWithInstanceGroups() {
        return executorWithInstanceConfiguration(new EmrInstanceGroups(instanceProperties));
    }

    private BulkImportExecutor executorWithInstanceGroupsSubnetIndexPicker(IntUnaryOperator randomSubnet) {
        return executorWithInstanceConfiguration(new EmrInstanceGroups(instanceProperties, randomSubnet));
    }

    private BulkImportExecutor executorWithInstanceFleets() {
        return executorWithInstanceConfiguration(new EmrInstanceFleets(instanceProperties));
    }

    private BulkImportJob singleFileJob() {
        return singleFileJobBuilder().build();
    }

    private BulkImportJob.Builder singleFileJobBuilder() {
        return new BulkImportJob.Builder()
                .tableId(tableId)
                .tableName(tableProperties.get(TABLE_NAME))
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"));
    }

    private Stream<InstanceGroupConfig> requestedInstanceGroups(InstanceRoleType roleType) {
        return requestedInstanceGroups()
                .filter(g -> roleType.name().equals(g.getInstanceRole()));
    }

    private Stream<InstanceGroupConfig> requestedInstanceGroups() {
        return requested.get().getInstances().getInstanceGroups().stream();
    }

    private ComputeLimits requestedComputeLimits() {
        return requested.get().getManagedScalingPolicy().getComputeLimits();
    }

    private String requestedInstanceGroupSubnetId() {
        return requested.get().getInstances().getEc2SubnetId();
    }

    private List<String> requestedInstanceFleetSubnetIds() {
        return requested.get().getInstances().getEc2SubnetIds();
    }

    private Stream<InstanceFleetConfig> requestedInstanceFleets() {
        return requested.get().getInstances().getInstanceFleets().stream();
    }

    private Stream<InstanceFleetConfig> requestedInstanceFleets(InstanceFleetType type) {
        return requestedInstanceFleets().filter(fleet -> type.name().equals(fleet.getInstanceFleetType()));
    }

    public static class FakeEmrInstanceConfiguration implements EmrInstanceConfiguration {
        @Override
        public JobFlowInstancesConfig createJobFlowInstancesConfig(EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
            return new JobFlowInstancesConfig();
        }

        @Override
        public ComputeLimits createComputeLimits(BulkImportPlatformSpec platformSpec) {
            return new ComputeLimits();
        }
    }
}
