/*
 * Copyright 2022 Crown Copyright
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
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS;

public class EmrExecutorTest {
    private AmazonElasticMapReduce emr;
    private TablePropertiesProvider tablePropertiesProvider;
    private AtomicReference<RunJobFlowRequest> requested;
    private AmazonS3 amazonS3;

    @Before
    public void setUpEmr() {
        requested = new AtomicReference<>();
        amazonS3 = mock(AmazonS3Client.class);
        emr = mock(AmazonElasticMapReduce.class);
        when(emr.runJobFlow(any(RunJobFlowRequest.class)))
                .then((Answer<RunJobFlowResult>) invocation -> {
                    requested.set(invocation.getArgument(0));
                    return new RunJobFlowResult();
                });
        tablePropertiesProvider = mock(TablePropertiesProvider.class);
        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> new TableProperties(new InstanceProperties()));
    }

    @Test
    public void shouldCreateAClusterOfThreeMachinesByDefault() {
        // Given
        EmrExecutor emrExecutor = new EmrExecutor(emr, new InstanceProperties(), tablePropertiesProvider, amazonS3);
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
        assertEquals(3, (int) instanceCount);
    }

    @Test
    public void shouldUseInstanceTypeDefinedInJob() {
        // Given
        EmrExecutor emrExecutor = new EmrExecutor(emr, new InstanceProperties(), tablePropertiesProvider, amazonS3);
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
        assertEquals("r5.xlarge", executorInstanceType);
    }

    @Test
    public void shouldUseDefaultMarketType() {
        // Given
        EmrExecutor emrExecutor = new EmrExecutor(emr, new InstanceProperties(), tablePropertiesProvider, amazonS3);
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
        assertEquals("SPOT", executorMarketType);
    }

    @Test
    public void shouldUseMarketTypeDefinedInConfig() {
        // Given
        TableProperties tableProperties  = new TableProperties(new InstanceProperties());
        tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS , "5");
        tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS , "10");
        tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE , "ON_DEMAND");

        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> tableProperties);


        EmrExecutor emrExecutor = new EmrExecutor(emr, new InstanceProperties(), tablePropertiesProvider, amazonS3);
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
        assertEquals("ON_DEMAND", executorMarketType);
    }

    @Test
    public void shouldUseMarketTypeDefinedInRequest() {
        // Given
        TableProperties tableProperties  = new TableProperties(new InstanceProperties());
        tableProperties.set(BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS , "5");
        tableProperties.set(BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS , "10");
        tableProperties.set(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE , "ON_DEMAND");

        when(tablePropertiesProvider.getTableProperties(anyString()))
                .then((Answer<TableProperties>) x -> tableProperties);

        Map<String,String>  platformSpec = new HashMap<>();
        platformSpec.put(BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE.getPropertyName() , "SPOT");

        EmrExecutor emrExecutor = new EmrExecutor(emr, new InstanceProperties(), tablePropertiesProvider, amazonS3);
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
        assertEquals("SPOT", executorMarketType);
    }

    @Test
    public void shouldEnableEMRManagedClusterScaling() {
        // Given
        EmrExecutor emrExecutor = new EmrExecutor(emr, new InstanceProperties(), tablePropertiesProvider, amazonS3);
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
        assertEquals(10, (int) scalingPolicy.getComputeLimits().getMaximumCapacityUnits());
        assertEquals(ComputeLimitsUnitType.Instances.name(), scalingPolicy.getComputeLimits().getUnitType());
    }

    @Test
    public void shouldUseUserProvidedConfigIfValuesOverrideDefaults() {
        // Given
        EmrExecutor emrExecutor = new EmrExecutor(emr, new InstanceProperties(), tablePropertiesProvider, amazonS3);
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

        assertEquals("100", conf.get("spark.hadoop.fs.s3a.connection.maximum"));
    }
}
