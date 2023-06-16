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
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceFleetType;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.InstanceTypeConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ManagedScalingPolicy;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.FixedStateStoreProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntUnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

class EmrExecutorTest {
    private AmazonElasticMapReduce emr;
    private AtomicReference<RunJobFlowRequest> requested;
    private AmazonS3 amazonS3;
    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final TableProperties tableProperties = new TableProperties(instanceProperties);

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
        instanceProperties.set(DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        instanceProperties.set(SUBNETS, "subnet-abc");
        tableProperties.set(TABLE_NAME, "myTable");
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
                            BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(),
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
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
            tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS, "10");
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
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
            tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS, "10");
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
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE, "m5.4xlarge,m5a.4xlarge");

            // When
            executorWithInstanceGroups().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceGroups(InstanceRoleType.CORE))
                    .extracting(InstanceGroupConfig::getInstanceType)
                    .containsExactly("m5.4xlarge");
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
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
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
            tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
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
            tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE, "m5.4xlarge,m5a.4xlarge");

            // When
            executorWithInstanceFleets().runJob(singleFileJob());

            // Then
            assertThat(requestedInstanceFleets(InstanceFleetType.CORE))
                    .flatExtracting(InstanceFleetConfig::getInstanceTypeConfigs)
                    .extracting(InstanceTypeConfig::getInstanceType)
                    .containsExactly("m5.4xlarge", "m5a.4xlarge");
        }
    }

    @Test
    void shouldEnableEMRManagedClusterScaling() {
        // Given
        BulkImportJob myJob = singleFileJobBuilder()
                .platformSpec(ImmutableMap.of(BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
                .build();

        // When
        executor().runJob(myJob);

        // Then
        ManagedScalingPolicy scalingPolicy = requested.get().getManagedScalingPolicy();
        assertThat(scalingPolicy).extracting(ManagedScalingPolicy::getComputeLimits)
                .extracting(ComputeLimits::getMaximumCapacityUnits, ComputeLimits::getUnitType)
                .containsExactly(10, ComputeLimitsUnitType.Instances.name());
    }

    @Test
    void shouldUseUserProvidedConfigIfValuesOverrideDefaults() {
        // Given
        BulkImportJob myJob = singleFileJobBuilder()
                .sparkConf(ImmutableMap.of("spark.hadoop.fs.s3a.connection.maximum", "100"))
                .platformSpec(ImmutableMap.of(BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
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

        // When
        executor().runJob(singleFileJob());

        // Then
        assertThat(requested.get())
                .isNull();
    }

    private EmrExecutor executor() {
        return executorWithInstanceConfiguration((ebsConfiguration, platformSpec) -> new JobFlowInstancesConfig());
    }

    private EmrExecutor executorWithInstanceConfiguration(EmrInstanceConfiguration configuration) {
        return new EmrExecutor(emr, instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties,
                        inMemoryStateStoreWithFixedSinglePartition(schemaWithKey("key"))),
                amazonS3, configuration);
    }

    private EmrExecutor executorWithInstanceGroups() {
        return executorWithInstanceConfiguration(new EmrInstanceGroups(instanceProperties));
    }

    private EmrExecutor executorWithInstanceGroupsSubnetIndexPicker(IntUnaryOperator randomSubnet) {
        return executorWithInstanceConfiguration(new EmrInstanceGroups(instanceProperties, randomSubnet));
    }

    private EmrExecutor executorWithInstanceFleets() {
        return executorWithInstanceConfiguration(new EmrInstanceFleets(instanceProperties));
    }

    private BulkImportJob singleFileJob() {
        return singleFileJobBuilder().build();
    }

    private BulkImportJob.Builder singleFileJobBuilder() {
        return new BulkImportJob.Builder()
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
}
