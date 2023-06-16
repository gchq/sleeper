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
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.statestore.FixedStateStoreProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
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
    private InstanceProperties instanceProperties;

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
        instanceProperties.set(BULK_IMPORT_BUCKET, "myBucket");
        instanceProperties.set(SUBNETS, "subnet-abc");
    }

    @Test
    void shouldCreateAClusterOfThreeMachinesByDefault() {
        // When
        executor().runJob(singleFileJob());

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
        BulkImportJob myJob = new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"))
                .platformSpec(ImmutableMap.of(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
                .build();

        // When
        executor().runJob(myJob);

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorInstanceType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getInstanceType).findFirst().orElse("not-found");
        assertThat(executorInstanceType).isEqualTo("r5.xlarge");
    }

    @Test
    void shouldUseDefaultMarketType() {
        // When
        executor().runJob(singleFileJob());

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorMarketType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getMarket).findFirst().orElse("not-found");
        assertThat(executorMarketType).isEqualTo("SPOT");
    }

    @Test
    void shouldUseMarketTypeDefinedInConfig() {
        // Given
        TableProperties tableProperties = tableProperties();
        tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
        tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS, "10");
        tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

        // When
        executorWithTableProperties(tableProperties).runJob(singleFileJob());

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorMarketType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getMarket).findFirst().orElse("not-found");
        assertThat(executorMarketType).isEqualTo("ON_DEMAND");
    }

    @Test
    void shouldUseMarketTypeDefinedInRequest() {
        // Given
        TableProperties tableProperties = tableProperties();
        tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, "5");
        tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS, "10");
        tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, "ON_DEMAND");

        Map<String, String> platformSpec = new HashMap<>();
        platformSpec.put(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE.getPropertyName(), "SPOT");

        BulkImportJob myJob = singleFileJobBuilder()
                .platformSpec(platformSpec).build();

        // When
        executorWithTableProperties(tableProperties).runJob(myJob);

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        String executorMarketType = config.getInstanceGroups().stream().filter(g -> g.getInstanceRole().equals(InstanceRoleType.CORE.name()))
                .map(InstanceGroupConfig::getMarket).findFirst().orElse("not-found");
        assertThat(executorMarketType).isEqualTo("SPOT");
    }

    @Test
    void shouldEnableEMRManagedClusterScaling() {
        // Given
        BulkImportJob myJob = singleFileJobBuilder()
                .platformSpec(ImmutableMap.of(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
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
                .platformSpec(ImmutableMap.of(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE.getPropertyName(), "r5.xlarge"))
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
        TableProperties tableProperties = tableProperties();
        tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "5");

        // When
        executorWithTableProperties(tableProperties).runJob(singleFileJob());

        // Then
        assertThat(requested.get())
                .isNull();
    }

    @Test
    void shouldSetSingleSubnetForInstanceGroup() {
        // Given
        instanceProperties.set(SUBNETS, "test-subnet");

        // When
        executor().runJob(singleFileJob());

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        assertThat(config.getEc2SubnetId()).isEqualTo("test-subnet");
    }

    @Test
    void shouldSetRandomSubnetForInstanceGroupWhenMultipleSubnetsSpecified() {
        // Given
        instanceProperties.set(SUBNETS, "test-subnet-1,test-subnet-2,test-subnet-3");
        int randomSubnetIndex = 1;

        // When
        executorWithRandomSubnetFunction(numSubnets -> randomSubnetIndex)
                .runJob(singleFileJob());

        // Then
        JobFlowInstancesConfig config = requested.get().getInstances();
        assertThat(config.getEc2SubnetId()).isEqualTo("test-subnet-2");
    }

    private EmrExecutor executor() {
        return executorWithTableProperties(tableProperties());
    }

    private EmrExecutor executorWithTableProperties(TableProperties tableProperties) {
        return new EmrExecutor(emr, instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties,
                        inMemoryStateStoreWithFixedSinglePartition(schemaWithKey("key"))),
                amazonS3);
    }

    private EmrExecutor executorWithRandomSubnetFunction(IntUnaryOperator randomSubnet) {
        return executorWithInstanceConfiguration(new EmrInstanceGroups(instanceProperties, randomSubnet));
    }

    private EmrExecutor executorWithInstanceConfiguration(EmrInstanceConfiguration configuration) {
        TableProperties tableProperties = tableProperties();
        return new EmrExecutor(emr, instanceProperties,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties,
                        inMemoryStateStoreWithFixedSinglePartition(schemaWithKey("key"))),
                amazonS3, configuration);
    }

    private BulkImportJob singleFileJob() {
        return singleFileJobBuilder().build();
    }

    private BulkImportJob.Builder singleFileJobBuilder() {
        return new BulkImportJob.Builder()
                .tableName("myTable")
                .id("my-job")
                .files(Lists.newArrayList("file1.parquet"));
    }

    private TableProperties tableProperties() {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "myTable");
        return tableProperties;
    }
}
