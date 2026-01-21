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
package sleeper.bulkimport.starter.executor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ComputeLimits;
import software.amazon.awssdk.services.emr.model.ComputeLimitsUnitType;
import software.amazon.awssdk.services.emr.model.EbsConfiguration;
import software.amazon.awssdk.services.emr.model.InstanceFleetConfig;
import software.amazon.awssdk.services.emr.model.InstanceFleetType;
import software.amazon.awssdk.services.emr.model.InstanceGroupConfig;
import software.amazon.awssdk.services.emr.model.InstanceRoleType;
import software.amazon.awssdk.services.emr.model.InstanceTypeConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.MarketType;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;

import sleeper.bulkimport.core.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.EmrInstanceArchitecture;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.job.status.JobStatusUpdateRecord;

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
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;

class EmrPlatformExecutorTest {
    private final EmrClient emr = mock(EmrClient.class);
    private final AtomicReference<RunJobFlowRequest> requested = new AtomicReference<>();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable().initialiseTable(tableProperties);
    private final String tableId = tableProperties.get(TABLE_ID);
    private final InMemoryIngestJobTracker tracker = new InMemoryIngestJobTracker();

    @BeforeEach
    public void setUpEmr() {
        when(emr.runJobFlow(any(RunJobFlowRequest.class)))
                .then((Answer<RunJobFlowResponse>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return RunJobFlowResponse.builder().build();
                });
        instanceProperties.set(DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE, EmrInstanceArchitecture.X86_64.toString());
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
                    .extracting(InstanceGroupConfig::instanceRole, InstanceGroupConfig::instanceCount)
                    .containsExactlyInAnyOrder(
                            tuple(InstanceRoleType.MASTER, 1),
                            tuple(InstanceRoleType.CORE, 2));
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
                    .extracting(InstanceGroupConfig::instanceType)
                    .containsExactly("r5.xlarge");
        }

        @Test
        void shouldUseDefaultMarketType() {
            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.CORE))
                    .extracting(InstanceGroupConfig::market)
                    .containsExactly(MarketType.SPOT);
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
                    .extracting(InstanceGroupConfig::market)
                    .containsExactly(MarketType.ON_DEMAND);
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
                    .extracting(InstanceGroupConfig::market)
                    .containsExactly(MarketType.SPOT);
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
                    .extracting(InstanceGroupConfig::instanceType)
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
                    .extracting(InstanceGroupConfig::instanceType)
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
                    .isEqualTo(ComputeLimits.builder()
                            .unitType(ComputeLimitsUnitType.INSTANCES)
                            .minimumCapacityUnits(1)
                            .maximumCapacityUnits(5)
                            .build());
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
                    .extracting(InstanceFleetConfig::instanceFleetType,
                            InstanceFleetConfig::targetOnDemandCapacity, InstanceFleetConfig::targetSpotCapacity)
                    .containsExactlyInAnyOrder(
                            tuple(InstanceFleetType.MASTER, 1, null),
                            tuple(InstanceFleetType.CORE, null, 2));
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
                    .extracting(InstanceFleetConfig::targetOnDemandCapacity, InstanceFleetConfig::targetSpotCapacity)
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
                    .extracting(InstanceFleetConfig::targetOnDemandCapacity, InstanceFleetConfig::targetSpotCapacity)
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
                    .flatExtracting(InstanceFleetConfig::instanceTypeConfigs)
                    .extracting(InstanceTypeConfig::instanceType)
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
                    .flatExtracting(InstanceFleetConfig::instanceTypeConfigs)
                    .extracting(InstanceTypeConfig::instanceType)
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
                    .isEqualTo(ComputeLimits.builder()
                            .unitType(ComputeLimitsUnitType.INSTANCE_FLEET_UNITS)
                            .minimumCapacityUnits(3)
                            .maximumCapacityUnits(5)
                            .build());
        }

        @Test
        void shouldSetCapacityWeightsForInstanceTypesForExecutorsWhenSpecified() {
            // Given
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES, "m5.4xlarge,5,m5a.4xlarge");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleets(InstanceFleetType.CORE))
                    .flatExtracting(InstanceFleetConfig::instanceTypeConfigs)
                    .extracting(InstanceTypeConfig::instanceType, InstanceTypeConfig::weightedCapacity)
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
        List<String> args = requested.get().steps().get(0).hadoopJarStep().args();
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
    void shouldReportJobRunIdToTracker() {
        // Given
        BulkImportJob myJob = singleFileJob();
        BulkImportExecutor executor = executorWithValidationTime(Instant.parse("2023-06-02T15:41:00Z"));

        // When
        executor.runJob(myJob, "test-job-run");

        // Then
        assertThat(tracker.getAllJobs(tableId))
                .containsExactly(ingestJobStatus(myJob.toIngestJob(),
                        acceptedRun(myJob.toIngestJob(), Instant.parse("2023-06-02T15:41:00Z"))));
        assertThat(tracker.streamTableRecords(tableId))
                .extracting(JobStatusUpdateRecord::getJobRunId)
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
        StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs);
        return new BulkImportExecutor(instanceProperties, tablePropertiesProvider, stateStoreProvider,
                tracker, (job, jobRunId) -> {
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
                .filter(g -> roleType.equals(g.instanceRole()));
    }

    private Stream<InstanceGroupConfig> requestedInstanceGroups() {
        return requested.get().instances().instanceGroups().stream();
    }

    private ComputeLimits requestedComputeLimits() {
        return requested.get().managedScalingPolicy().computeLimits();
    }

    private String requestedInstanceGroupSubnetId() {
        return requested.get().instances().ec2SubnetId();
    }

    private List<String> requestedInstanceFleetSubnetIds() {
        return requested.get().instances().ec2SubnetIds();
    }

    private Stream<InstanceFleetConfig> requestedInstanceFleets() {
        return requested.get().instances().instanceFleets().stream();
    }

    private Stream<InstanceFleetConfig> requestedInstanceFleets(InstanceFleetType type) {
        return requestedInstanceFleets().filter(fleet -> type.equals(fleet.instanceFleetType()));
    }

    public static class FakeEmrInstanceConfiguration implements EmrInstanceConfiguration {
        @Override
        public JobFlowInstancesConfig createJobFlowInstancesConfig(EbsConfiguration ebsConfiguration, BulkImportPlatformSpec platformSpec) {
            return JobFlowInstancesConfig.builder().build();
        }

        @Override
        public ComputeLimits createComputeLimits(BulkImportPlatformSpec platformSpec) {
            return ComputeLimits.builder().build();
        }
    }
}
